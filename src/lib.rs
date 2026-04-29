//! Plexus engine — graph execution runtime for the Plexus-Loom backend.
//!
//! Provides:
//! - [`Graph`] — the live container that holds blocks, edges, atoms, and port
//!   values and drives the execution loop.
//! - [`Executor`] — evaluates a single edge against the current port-value map.
//! - [`Scheduler`] — decides which edges fire and when (OnChange / OnTick / OnEvent).
//! - [`toposort`] — Kahn's algorithm for ordering edges by data dependency.
//! - [`TransformRegistry`] / [`eval_transform`] — named and inline transforms.
//! - [`EngineError`] — unified error type.

pub mod error;
pub mod executor;
pub mod graph;
pub mod scheduler;
pub mod toposort;
pub mod transform;

pub use error::EngineError;
pub use executor::Executor;
pub use graph::Graph;
pub use scheduler::Scheduler;
pub use toposort::toposort;
pub use transform::{BoxFuture, TransformRegistry, eval_transform};
