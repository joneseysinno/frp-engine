//! Edge executor — collects input values, runs the transform, and writes outputs.

use std::collections::HashMap;

use loom_domain::HyperEdge;
use plexus_base::{PortId, Value};

use crate::error::EngineError;
use crate::transform::{TransformRegistry, eval_transform};

/// Stateless executor that drives a single [`HyperEdge`].
pub struct Executor {
    pub registry: TransformRegistry,
}

impl Executor {
    /// Create an executor backed by the given transform registry.
    pub fn new(registry: TransformRegistry) -> Self {
        Executor { registry }
    }

    /// Execute one edge.
    ///
    /// 1. Collect input values from `port_values[sources]`.  Missing source
    ///    ports resolve to `Value::Null`.
    /// 2. Evaluate the edge transform (may be async).
    /// 3. Write the result into `port_values` for every target port.
    pub async fn execute(
        &self,
        edge: &HyperEdge,
        port_values: &mut HashMap<PortId, Value>,
    ) -> Result<(), EngineError> {
        let inputs: Vec<Value> = edge
            .sources
            .iter()
            .map(|pid| port_values.get(pid).cloned().unwrap_or(Value::Null))
            .collect();

        let result = eval_transform(&edge.transform, inputs, &self.registry).await?;

        for &target in &edge.targets {
            port_values.insert(target, result.clone());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loom_domain::{EdgeSchedule, EdgeTransform, HyperEdge};
    use plexus_base::{EdgeId, PortId, Value};
    use std::sync::Arc;

    fn make_edge(
        id: u64,
        sources: &[u64],
        targets: &[u64],
        transform: EdgeTransform,
    ) -> HyperEdge {
        HyperEdge::new(
            EdgeId::new(id),
            sources.iter().map(|&p| PortId::new(p)).collect(),
            targets.iter().map(|&p| PortId::new(p)).collect(),
            transform,
            EdgeSchedule::OnChange,
        )
    }

    #[tokio::test]
    async fn passthrough_copies_value() {
        let exec = Executor::new(TransformRegistry::new());
        let edge = make_edge(1, &[1], &[2], EdgeTransform::PassThrough);
        let mut ports = HashMap::new();
        ports.insert(PortId::new(1), Value::Int(42));

        exec.execute(&edge, &mut ports).await.unwrap();

        assert_eq!(ports[&PortId::new(2)], Value::Int(42));
    }

    #[tokio::test]
    async fn missing_source_resolves_to_null() {
        let exec = Executor::new(TransformRegistry::new());
        let edge = make_edge(1, &[99], &[2], EdgeTransform::PassThrough);
        let mut ports = HashMap::new();

        exec.execute(&edge, &mut ports).await.unwrap();

        assert_eq!(ports[&PortId::new(2)], Value::Null);
    }

    #[tokio::test]
    async fn named_transform_applied() {
        let mut reg = TransformRegistry::new();
        reg.register("negate", |inputs| {
            if let Some(Value::Int(n)) = inputs.first() {
                Value::Int(-n)
            } else {
                Value::Null
            }
        });
        let exec = Executor::new(reg);
        let edge = make_edge(1, &[1], &[2], EdgeTransform::Named("negate".to_string()));
        let mut ports = HashMap::new();
        ports.insert(PortId::new(1), Value::Int(7));

        exec.execute(&edge, &mut ports).await.unwrap();

        assert_eq!(ports[&PortId::new(2)], Value::Int(-7));
    }

    #[tokio::test]
    async fn multiple_targets_all_written() {
        let exec = Executor::new(TransformRegistry::new());
        let edge = make_edge(1, &[1], &[2, 3, 4], EdgeTransform::PassThrough);
        let mut ports = HashMap::new();
        ports.insert(PortId::new(1), Value::Bool(true));

        exec.execute(&edge, &mut ports).await.unwrap();

        assert_eq!(ports[&PortId::new(2)], Value::Bool(true));
        assert_eq!(ports[&PortId::new(3)], Value::Bool(true));
        assert_eq!(ports[&PortId::new(4)], Value::Bool(true));
    }

    #[tokio::test]
    async fn inline_transform_applied() {
        let exec = Executor::new(TransformRegistry::new());
        let t = EdgeTransform::Inline(Arc::new(|_| Value::Int(100)));
        let edge = make_edge(1, &[], &[5], t);
        let mut ports = HashMap::new();

        exec.execute(&edge, &mut ports).await.unwrap();

        assert_eq!(ports[&PortId::new(5)], Value::Int(100));
    }
}
