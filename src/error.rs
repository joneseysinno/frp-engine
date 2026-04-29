//! Error types for the frp-engine crate.

use frp_plexus::{BlockId, EdgeId, PortId};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("cycle detected in edge graph")]
    CycleDetected,

    #[error("transform not found: '{0}'")]
    TransformNotFound(String),

    #[error("execution failed: {0}")]
    ExecutionFailed(String),

    #[error("port not found: {0:?}")]
    PortNotFound(PortId),

    #[error("block not found: {0:?}")]
    BlockNotFound(BlockId),

    #[error("edge not found: {0:?}")]
    EdgeNotFound(EdgeId),

    #[error("validation failed: {0}")]
    ValidationFailed(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cycle_detected_display() {
        let e = EngineError::CycleDetected;
        assert_eq!(e.to_string(), "cycle detected in edge graph");
    }

    #[test]
    fn transform_not_found_display() {
        let e = EngineError::TransformNotFound("my_fn".to_string());
        assert_eq!(e.to_string(), "transform not found: 'my_fn'");
    }

    #[test]
    fn port_not_found_display() {
        let e = EngineError::PortNotFound(PortId::new(7));
        assert!(e.to_string().contains("7"));
    }
}
