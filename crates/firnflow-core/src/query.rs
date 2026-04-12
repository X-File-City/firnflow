//! Query and index request types.
//!
//! Request payloads are kept in their own module so that `result.rs`
//! stays focused on the response side. Both modules share the same
//! serde derives and are what the axum handlers parse straight from
//! request bodies.

use serde::{Deserialize, Serialize};

/// Default number of IVF partitions to probe per query when an
/// index exists. Matches lancedb's own default (20).
pub const DEFAULT_NPROBES: usize = 20;

/// Parameters of a vector nearest-neighbour query.
///
/// The `vector` must match the namespace's established dimension;
/// validation happens at the manager boundary. `k` bounds the
/// number of results the caller wants back.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryRequest {
    /// The query vector.
    pub vector: Vec<f32>,
    /// Maximum number of results to return.
    pub k: usize,
    /// Number of IVF partitions to probe. Only meaningful when an
    /// index exists; ignored for linear scans. Defaults to 20 if
    /// omitted.
    #[serde(default)]
    pub nprobes: Option<usize>,
}

/// Parameters for an explicit index build request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexRequest {
    /// Index type. Currently only `"ivf_pq"` is supported.
    #[serde(default = "default_index_kind")]
    pub kind: String,
    /// Number of IVF partitions. Defaults to `sqrt(row_count)` if
    /// omitted.
    pub num_partitions: Option<u32>,
    /// Number of PQ sub-vectors. Defaults to `dim / 16` if omitted.
    pub num_sub_vectors: Option<u32>,
}

fn default_index_kind() -> String {
    "ivf_pq".into()
}
