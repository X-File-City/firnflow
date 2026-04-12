//! Request handlers for the firnflow REST API.
//!
//! * `GET    /health`
//! * `POST   /ns/{namespace}/upsert`
//! * `POST   /ns/{namespace}/query`
//! * `DELETE /ns/{namespace}`
//! * `POST   /ns/{namespace}/warmup`
//! * `POST   /ns/{namespace}/index`
//! * `GET    /metrics`

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};

use firnflow_core::{IndexRequest, NamespaceId, QueryRequest, QueryResultSet};

use crate::error::ApiError;
use crate::state::AppState;

/// Body of a successful delete response.
#[derive(Debug, Serialize)]
pub struct DeleteResponse {
    /// Number of S3 objects removed during the delete.
    pub objects_deleted: usize,
}

/// Body of `POST /ns/{namespace}/warmup`. A list of query
/// parameters the operator wants pre-populated in the cache.
#[derive(Debug, Deserialize)]
pub struct WarmupRequest {
    /// Queries to run through the cache-aside path as a background
    /// task. The handler accepts the request and spawns a task
    /// that iterates through this list; per-query failures are
    /// logged via `tracing::warn!` and do not abort the warmup.
    pub queries: Vec<QueryRequest>,
}

/// Body of a successful warmup response (HTTP 202 Accepted). The
/// number is how many queries the background task was *asked*
/// to run, not how many actually succeeded by the time the
/// response is returned — the task runs after the response is
/// sent.
#[derive(Debug, Serialize)]
pub struct WarmupResponse {
    pub queued: usize,
}

/// One row in an upsert request.
#[derive(Debug, Deserialize)]
pub struct UpsertRow {
    pub id: u64,
    pub vector: Vec<f32>,
}

/// Body of `POST /ns/{namespace}/upsert`.
#[derive(Debug, Deserialize)]
pub struct UpsertRequest {
    pub rows: Vec<UpsertRow>,
}

/// Body of a successful upsert response.
#[derive(Debug, Serialize)]
pub struct UpsertResponse {
    /// Number of rows accepted for append. Matches `rows.len()` on the
    /// request — there is no per-row failure reporting in slice 1c.
    pub upserted: usize,
}

/// Liveness probe. Returns HTTP 200 with body `ok`.
pub async fn health() -> &'static str {
    "ok"
}

/// Append rows to a namespace and invalidate its cached query results.
pub async fn upsert(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    Json(req): Json<UpsertRequest>,
) -> Result<Json<UpsertResponse>, ApiError> {
    let ns = NamespaceId::new(namespace)?;
    let count = req.rows.len();
    let rows: Vec<(u64, Vec<f32>)> = req.rows.into_iter().map(|r| (r.id, r.vector)).collect();
    state.service.upsert(&ns, rows).await?;
    Ok(Json(UpsertResponse { upserted: count }))
}

/// Run a vector nearest-neighbour query through the cache-aside path.
pub async fn query(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResultSet>, ApiError> {
    let ns = NamespaceId::new(namespace)?;
    let result = state.service.query(&ns, &req).await?;
    Ok(Json(result))
}

/// Delete a namespace: remove every S3 object under its prefix and
/// evict every cached query result for it. Returns the count of
/// S3 objects the manager actually deleted.
pub async fn delete(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
) -> Result<Json<DeleteResponse>, ApiError> {
    let ns = NamespaceId::new(namespace)?;
    let objects_deleted = state.service.delete(&ns).await?;
    Ok(Json(DeleteResponse { objects_deleted }))
}

/// Async cache-warmup hint.
///
/// CLAUDE.md: *"the warmup endpoint must be non-blocking: it
/// spawns an async task and returns 202 immediately"*.
///
/// The handler validates the namespace, spawns a `tokio::task`
/// that runs each query from the request body through
/// [`NamespaceService::query`] (populating the cache as it
/// goes), and returns `202 Accepted` with the number of queries
/// queued. Failures inside the background task are logged via
/// `tracing::warn!` — they do not affect the HTTP response or
/// abort the rest of the warmup batch.
pub async fn warmup(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    Json(req): Json<WarmupRequest>,
) -> Result<(StatusCode, Json<WarmupResponse>), ApiError> {
    let ns = NamespaceId::new(namespace)?;
    let queued = req.queries.len();

    let service = Arc::clone(&state.service);
    let ns_owned = ns.clone();
    let queries = req.queries;
    tokio::spawn(async move {
        for (idx, query) in queries.iter().enumerate() {
            if let Err(e) = service.query(&ns_owned, query).await {
                tracing::warn!(
                    namespace = %ns_owned,
                    query_index = idx,
                    error = %e,
                    "warmup query failed"
                );
            }
        }
    });

    Ok((StatusCode::ACCEPTED, Json(WarmupResponse { queued })))
}

/// Body of a successful index build response (HTTP 202 Accepted).
#[derive(Debug, Serialize)]
pub struct IndexResponse {
    /// Confirmation that the build was queued.
    pub status: String,
}

/// Explicit ANN index build (slice 6b).
///
/// Spawns a background task that builds an IVF_PQ index on the
/// namespace's vector column and returns `202 Accepted` immediately.
/// Same fire-and-forget pattern as warmup. Operators monitor the
/// `firnflow_index_build_duration_seconds` histogram to know when
/// the build completes.
pub async fn create_index(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    Json(req): Json<IndexRequest>,
) -> Result<(StatusCode, Json<IndexResponse>), ApiError> {
    let ns = NamespaceId::new(namespace)?;

    if req.kind != "ivf_pq" {
        return Err(ApiError(firnflow_core::FirnflowError::InvalidRequest(
            format!(
                "unsupported index kind {:?}, only \"ivf_pq\" is supported",
                req.kind
            ),
        )));
    }

    let service = Arc::clone(&state.service);
    let ns_owned = ns.clone();
    tokio::spawn(async move {
        if let Err(e) = service
            .create_index(&ns_owned, req.num_partitions, req.num_sub_vectors)
            .await
        {
            tracing::error!(
                namespace = %ns_owned,
                error = %e,
                "index build failed"
            );
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(IndexResponse {
            status: "index build queued".into(),
        }),
    ))
}

/// Body of a successful compact response (HTTP 202 Accepted).
#[derive(Debug, Serialize)]
pub struct CompactResponse {
    /// Confirmation that the compaction was queued.
    pub status: String,
}

/// Explicit compaction (slice 6c).
///
/// Spawns a background task that merges small data files into
/// fewer, larger ones and returns `202 Accepted` immediately.
/// Operators monitor the `firnflow_compaction_duration_seconds`
/// histogram to know when the compaction completes.
pub async fn compact(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
) -> Result<(StatusCode, Json<CompactResponse>), ApiError> {
    let ns = NamespaceId::new(namespace)?;

    let service = Arc::clone(&state.service);
    let ns_owned = ns.clone();
    tokio::spawn(async move {
        match service.compact(&ns_owned).await {
            Ok(result) => {
                tracing::info!(
                    namespace = %ns_owned,
                    fragments_removed = result.fragments_removed,
                    fragments_added = result.fragments_added,
                    "compaction complete"
                );
            }
            Err(e) => {
                tracing::error!(
                    namespace = %ns_owned,
                    error = %e,
                    "compaction failed"
                );
            }
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(CompactResponse {
            status: "compaction queued".into(),
        }),
    ))
}

/// Prometheus scrape endpoint. Serialises the process-wide
/// [`CoreMetrics`] registry into the Prometheus text exposition
/// format with a `text/plain; version=0.0.4` content type.
pub async fn metrics(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let body = state.metrics.encode()?;
    Ok((
        [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    ))
}
