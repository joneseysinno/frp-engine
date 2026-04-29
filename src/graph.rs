//! [`Graph`] — the live container that ties together blocks, edges, atoms,
//! port values, scheduling, and execution.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use futures::future::try_join_all;
use loom_domain::{Atom, Block, HyperEdge};
use plexus_base::{AtomId, BlockId, EdgeId, GraphId, PortId, Value};

use crate::error::EngineError;
use crate::executor::Executor;
use crate::scheduler::Scheduler;
use crate::toposort::toposort;
use crate::transform::{TransformRegistry, eval_transform};

/// A live, mutable graph of blocks, edges, and atoms.
///
/// `Graph` owns all domain objects and drives the execution loop.  Typical
/// usage:
/// 1. Create via `Graph::new`.
/// 2. Add blocks / edges / atoms.
/// 3. Call `set_port_value` to inject inputs.
/// 4. Call `run_pending` to flush the scheduler queue.
/// 5. Read output values via `get_port_value`.
pub struct Graph {
    /// Unique id for this graph.
    pub id: GraphId,
    /// All blocks keyed by id.
    pub blocks: HashMap<BlockId, Block>,
    /// All edges keyed by id.
    pub edges: HashMap<EdgeId, HyperEdge>,
    /// All atoms keyed by id.
    pub atoms: HashMap<AtomId, Atom>,
    /// Current port values.
    pub port_values: HashMap<PortId, Value>,
    /// Edges in topological execution order (rebuilt on structural changes).
    /// Delay edges are excluded — they are tracked separately.
    sorted_edges: Vec<EdgeId>,
    /// IDs of edges marked `delay: true`.
    delay_edge_ids: HashSet<EdgeId>,
    /// Pending output values from delay edges; flushed into `port_values` at
    /// the start of the next `run_pending` call.
    delay_buffer: HashMap<PortId, Value>,
    scheduler: Scheduler,
    executor: Executor,
}

impl Graph {
    /// Create an empty graph with the given id and transform registry.
    pub fn new(id: GraphId, registry: TransformRegistry) -> Self {
        Graph {
            id,
            blocks: HashMap::new(),
            edges: HashMap::new(),
            atoms: HashMap::new(),
            port_values: HashMap::new(),
            sorted_edges: Vec::new(),
            delay_edge_ids: HashSet::new(),
            delay_buffer: HashMap::new(),
            scheduler: Scheduler::new(),
            executor: Executor::new(registry),
        }
    }

    // ── structural mutations ──────────────────────────────────────────────

    /// Add a block to the graph.
    pub fn add_block(&mut self, block: Block) {
        self.blocks.insert(block.id, block);
    }

    /// Add an edge to the graph and register it with the scheduler.
    ///
    /// Delay edges (`edge.delay == true`) are stored in `delay_edge_ids` and
    /// excluded from the topological sort, allowing them to participate in
    /// feedback cycles without triggering `CycleDetected`.
    pub fn add_edge(&mut self, edge: HyperEdge) -> Result<(), EngineError> {
        self.scheduler.register(&edge);
        if edge.delay {
            self.delay_edge_ids.insert(edge.id);
        }
        self.edges.insert(edge.id, edge);
        self.rebuild_sort()
    }

    /// Remove a block by id (no-op if not present).
    pub fn remove_block(&mut self, id: BlockId) {
        self.blocks.remove(&id);
    }

    /// Remove an edge by id and rebuild the topological sort.
    pub fn remove_edge(&mut self, id: EdgeId) -> Result<(), EngineError> {
        self.edges.remove(&id);
        self.delay_edge_ids.remove(&id);
        self.rebuild_sort()
    }

    /// Add an atom to the graph.
    pub fn add_atom(&mut self, atom: Atom) {
        self.atoms.insert(atom.id, atom);
    }

    // ── port value access ─────────────────────────────────────────────────

    /// Write a port value and notify the scheduler.
    pub fn set_port_value(&mut self, port: PortId, value: Value) {
        self.port_values.insert(port, value);
        self.scheduler.notify_change(port);
    }

    /// Read a port value (returns `None` if not set).
    pub fn get_port_value(&self, port: PortId) -> Option<&Value> {
        self.port_values.get(&port)
    }

    // ── execution ─────────────────────────────────────────────────────────

    /// Advance the tick-based scheduler by `delta` and run any newly pending edges.
    pub async fn tick(&mut self, delta: Duration) -> Result<(), EngineError> {
        self.scheduler.tick(delta);
        self.run_pending().await
    }

    /// Fire a named event and run any newly pending edges.
    pub async fn fire_event(&mut self, name: &str) -> Result<(), EngineError> {
        self.scheduler.fire_event(name);
        self.run_pending().await
    }

    /// Drain the scheduler queue and execute pending edges in parallel waves.
    ///
    /// **Delay buffer flush:** at the start of each call the delay buffer from
    /// the previous tick is applied to `port_values` and its ports are
    /// notified, so their downstream normal edges are included in this tick's
    /// execution.
    ///
    /// **Normal edges** are grouped into independent waves by
    /// [`compute_levels`](Self::compute_levels) and executed concurrently
    /// within each wave via [`try_join_all`].
    ///
    /// **Delay edges** run after all normal waves.  Their outputs are written
    /// into the delay buffer rather than `port_values`, deferring the effect
    /// to the next tick.
    pub async fn run_pending(&mut self) -> Result<(), EngineError> {
        // ── 1. Flush delay buffer from previous tick ──────────────────────
        if !self.delay_buffer.is_empty() {
            let flushed: Vec<(PortId, Value)> = self.delay_buffer.drain().collect();
            for (port, val) in flushed {
                self.port_values.insert(port, val);
                self.scheduler.notify_change(port);
            }
        }

        // ── 2. Drain scheduler (includes edges triggered by the flush) ────
        let pending = self.scheduler.drain_pending();
        if pending.is_empty() {
            return Ok(());
        }

        let pending_set: HashSet<EdgeId> = pending.into_iter().collect();

        // ── 3. Partition into normal vs delay ─────────────────────────────
        let normal_set: HashSet<EdgeId> = pending_set
            .iter()
            .copied()
            .filter(|id| !self.delay_edge_ids.contains(id))
            .collect();
        let delay_set: HashSet<EdgeId> = pending_set
            .iter()
            .copied()
            .filter(|id| self.delay_edge_ids.contains(id))
            .collect();

        // ── 4. Execute normal waves in parallel ───────────────────────────
        if !normal_set.is_empty() {
            let waves = Self::compute_levels(&normal_set, &self.sorted_edges, &self.edges);

            for wave in waves {
                let tasks: Vec<(HyperEdge, Vec<Value>)> = wave
                    .iter()
                    .map(|&eid| {
                        let edge = self.edges[&eid].clone();
                        let inputs: Vec<Value> = edge
                            .sources
                            .iter()
                            .map(|pid| self.port_values.get(pid).cloned().unwrap_or(Value::Null))
                            .collect();
                        (edge, inputs)
                    })
                    .collect();

                let futures_iter = tasks
                    .iter()
                    .map(|(edge, inputs)| eval_transform(&edge.transform, inputs.clone(), &self.executor.registry));
                let results: Vec<Value> = try_join_all(futures_iter).await?;

                for ((edge, _), result) in tasks.iter().zip(results.iter()) {
                    for &target in &edge.targets {
                        self.port_values.insert(target, result.clone());
                    }
                }
            }
        }

        // ── 5. Execute delay edges — buffer outputs for next tick ─────────
        for &eid in &delay_set {
            let edge = match self.edges.get(&eid) {
                Some(e) => e.clone(),
                None => continue,
            };
            let inputs: Vec<Value> = edge
                .sources
                .iter()
                .map(|pid| self.port_values.get(pid).cloned().unwrap_or(Value::Null))
                .collect();
            let result = eval_transform(&edge.transform, inputs, &self.executor.registry).await?;
            for &target in &edge.targets {
                self.delay_buffer.insert(target, result.clone());
            }
        }

        Ok(())
    }

    // ── helpers ───────────────────────────────────────────────────────────

    /// Group `pending_set` edges into independent waves (levels) based on their
    /// inter-dependencies within the pending set.
    ///
    /// Edges with no pending predecessors are assigned level 0 and can run in
    /// parallel.  An edge whose pending predecessor has level *k* gets level
    /// *k + 1*.  Non-pending predecessors are ignored because their outputs are
    /// already settled in `port_values` before `run_pending` is called.
    fn compute_levels(
        pending_set: &HashSet<EdgeId>,
        sorted_edges: &[EdgeId],
        edges: &HashMap<EdgeId, HyperEdge>,
    ) -> Vec<Vec<EdgeId>> {
        // Map: output port → pending edge that produces it.
        let mut pending_producers: HashMap<PortId, EdgeId> = HashMap::new();
        for &eid in pending_set {
            if let Some(edge) = edges.get(&eid) {
                for &port in &edge.targets {
                    pending_producers.insert(port, eid);
                }
            }
        }

        let mut level_of: HashMap<EdgeId, usize> = HashMap::new();
        let mut max_level = 0usize;

        // Walk in topo order so predecessors are always assigned before successors.
        for &eid in sorted_edges {
            if !pending_set.contains(&eid) {
                continue;
            }
            let edge = match edges.get(&eid) {
                Some(e) => e,
                None => continue,
            };
            let level = edge
                .sources
                .iter()
                .filter_map(|port| pending_producers.get(port))
                .filter_map(|pred| level_of.get(pred))
                .copied()
                .max()
                .map_or(0, |l| l + 1);
            level_of.insert(eid, level);
            if level > max_level {
                max_level = level;
            }
        }

        let mut waves: Vec<Vec<EdgeId>> = vec![Vec::new(); max_level + 1];
        // Re-walk in topo order so edges within a wave are also topo-ordered
        // (makes behaviour deterministic and easier to reason about).
        for &eid in sorted_edges {
            if let Some(&level) = level_of.get(&eid) {
                waves[level].push(eid);
            }
        }

        waves
    }

    fn rebuild_sort(&mut self) -> Result<(), EngineError> {
        // Delay edges are excluded: they may form cycles and must not be
        // fed to Kahn's algorithm.
        let normal_edges: Vec<HyperEdge> = self
            .edges
            .values()
            .filter(|e| !e.delay)
            .cloned()
            .collect();
        self.sorted_edges = toposort(&normal_edges)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loom_domain::{
        Atom, AtomKind, AtomMeta, Block, BlockSchema, EdgeSchedule, EdgeTransform, HyperEdge,
        Meta,
    };
    use plexus_base::{
        AtomId, BlockId, EdgeId, GraphId, LayerTag, PortId, Value,
    };

    fn simple_edge(id: u64, src: u64, tgt: u64) -> HyperEdge {
        HyperEdge::new(
            EdgeId::new(id),
            vec![PortId::new(src)],
            vec![PortId::new(tgt)],
            EdgeTransform::PassThrough,
            EdgeSchedule::OnChange,
        )
    }

    fn simple_block(id: u64) -> Block {
        Block {
            id: BlockId::new(id),
            schema: BlockSchema::new(vec![], vec![]),
            atoms: vec![],
            meta: Meta::default(),
        }
    }

    fn simple_atom(id: u64) -> Atom {
        Atom::new(
            AtomId::new(id),
            AtomKind::Transform,
            AtomMeta::new("test".to_string(), LayerTag::Core),
        )
    }

    #[test]
    fn add_and_remove_block() {
        let mut g = Graph::new(GraphId::new(1), TransformRegistry::new());
        g.add_block(simple_block(10));
        assert!(g.blocks.contains_key(&BlockId::new(10)));
        g.remove_block(BlockId::new(10));
        assert!(!g.blocks.contains_key(&BlockId::new(10)));
    }

    #[test]
    fn add_edge_sorts_topologically() {
        let mut g = Graph::new(GraphId::new(1), TransformRegistry::new());
        // edge 2 depends on port 5 which is written by edge 1
        g.add_edge(HyperEdge::new(
            EdgeId::new(2),
            vec![PortId::new(5)],
            vec![PortId::new(6)],
            EdgeTransform::PassThrough,
            EdgeSchedule::OnChange,
        ))
        .unwrap();
        g.add_edge(HyperEdge::new(
            EdgeId::new(1),
            vec![],
            vec![PortId::new(5)],
            EdgeTransform::PassThrough,
            EdgeSchedule::OnChange,
        ))
        .unwrap();

        let pos_1 = g.sorted_edges.iter().position(|&id| id == EdgeId::new(1)).unwrap();
        let pos_2 = g.sorted_edges.iter().position(|&id| id == EdgeId::new(2)).unwrap();
        assert!(pos_1 < pos_2);
    }

    #[test]
    fn set_and_get_port_value() {
        let mut g = Graph::new(GraphId::new(1), TransformRegistry::new());
        g.set_port_value(PortId::new(1), Value::Int(99));
        assert_eq!(g.get_port_value(PortId::new(1)), Some(&Value::Int(99)));
        assert_eq!(g.get_port_value(PortId::new(2)), None);
    }

    #[tokio::test]
    async fn run_pending_propagates_value() {
        let mut g = Graph::new(GraphId::new(1), TransformRegistry::new());
        g.add_edge(simple_edge(1, 10, 20)).unwrap();

        // set_port_value enqueues edge 1 (OnChange)
        g.set_port_value(PortId::new(10), Value::Int(7));
        g.run_pending().await.unwrap();

        assert_eq!(g.get_port_value(PortId::new(20)), Some(&Value::Int(7)));
    }

    #[tokio::test]
    async fn tick_fires_on_tick_edge() {
        let mut g = Graph::new(GraphId::new(1), TransformRegistry::new());
        g.add_edge(HyperEdge::new(
            EdgeId::new(1),
            vec![PortId::new(1)],
            vec![PortId::new(2)],
            EdgeTransform::PassThrough,
            EdgeSchedule::OnTick(Duration::from_millis(100)),
        ))
        .unwrap();
        g.port_values.insert(PortId::new(1), Value::Bool(true));

        g.tick(Duration::from_millis(150)).await.unwrap();
        assert_eq!(g.get_port_value(PortId::new(2)), Some(&Value::Bool(true)));
    }

    #[tokio::test]
    async fn fire_event_triggers_edge() {
        let mut g = Graph::new(GraphId::new(1), TransformRegistry::new());
        g.add_edge(HyperEdge::new(
            EdgeId::new(1),
            vec![PortId::new(1)],
            vec![PortId::new(2)],
            EdgeTransform::PassThrough,
            EdgeSchedule::OnEvent("ping".to_string()),
        ))
        .unwrap();
        g.port_values.insert(PortId::new(1), Value::Int(42));

        g.fire_event("ping").await.unwrap();
        assert_eq!(g.get_port_value(PortId::new(2)), Some(&Value::Int(42)));
    }

    #[test]
    fn cycle_in_edges_returns_error() {
        let mut g = Graph::new(GraphId::new(1), TransformRegistry::new());
        g.add_edge(HyperEdge::new(
            EdgeId::new(1),
            vec![PortId::new(2)],
            vec![PortId::new(1)],
            EdgeTransform::PassThrough,
            EdgeSchedule::OnChange,
        ))
        .unwrap();
        let err = g
            .add_edge(HyperEdge::new(
                EdgeId::new(2),
                vec![PortId::new(1)],
                vec![PortId::new(2)],
                EdgeTransform::PassThrough,
                EdgeSchedule::OnChange,
            ))
            .unwrap_err();
        assert!(matches!(err, EngineError::CycleDetected));
    }

    #[test]
    fn add_atom_stored() {
        let mut g = Graph::new(GraphId::new(1), TransformRegistry::new());
        g.add_atom(simple_atom(5));
        assert!(g.atoms.contains_key(&AtomId::new(5)));
    }

    /// A delay edge's output must not appear in `port_values` until the
    /// *second* `run_pending` call (one tick later).
    #[tokio::test]
    async fn delay_edge_output_deferred_one_tick() {
        let mut g = Graph::new(GraphId::new(1), TransformRegistry::new());

        // Delay edge: port 1 → port 2.
        g.add_edge(
            HyperEdge::new(
                EdgeId::new(1),
                vec![PortId::new(1)],
                vec![PortId::new(2)],
                EdgeTransform::PassThrough,
                EdgeSchedule::OnChange,
            )
            .with_delay(),
        )
        .unwrap();

        g.set_port_value(PortId::new(1), Value::Int(99));

        // Tick 1: delay edge fires, result goes to buffer, NOT port_values.
        g.run_pending().await.unwrap();
        assert_eq!(g.get_port_value(PortId::new(2)), None,
            "delay edge output must not appear until the next tick");

        // Tick 2: buffer is flushed at the start of run_pending.
        // No new pending edges, but the flush itself writes port 2.
        g.run_pending().await.unwrap();
        assert_eq!(g.get_port_value(PortId::new(2)), Some(&Value::Int(99)));
    }

    /// A feedback cycle (normal edge A→B, delay edge B→A) must not produce
    /// `CycleDetected` and must propagate values correctly across ticks.
    #[tokio::test]
    async fn delay_edge_allows_feedback_cycle() {
        let mut g = Graph::new(GraphId::new(1), TransformRegistry::new());

        // Normal edge: port 10 → port 20.
        g.add_edge(HyperEdge::new(
            EdgeId::new(1),
            vec![PortId::new(10)],
            vec![PortId::new(20)],
            EdgeTransform::PassThrough,
            EdgeSchedule::OnChange,
        ))
        .unwrap();

        // Delay feedback edge: port 20 → port 10 (completes the cycle).
        // Both add_edge calls must succeed — no CycleDetected.
        g.add_edge(
            HyperEdge::new(
                EdgeId::new(2),
                vec![PortId::new(20)],
                vec![PortId::new(10)],
                EdgeTransform::PassThrough,
                EdgeSchedule::OnChange,
            )
            .with_delay(),
        )
        .unwrap();

        // Tick 1: inject 5 into port 10.
        // Normal edge fires → port 20 = 5.
        // Delay edge fires → delay_buffer[port 10] = 5.
        g.set_port_value(PortId::new(10), Value::Int(5));
        g.run_pending().await.unwrap();
        assert_eq!(g.get_port_value(PortId::new(20)), Some(&Value::Int(5)));
        // Port 10 is still the externally-set value, not yet overwritten.
        assert_eq!(g.get_port_value(PortId::new(10)), Some(&Value::Int(5)));

        // Tick 2: flush writes port 10 = 5 (feedback), which re-triggers
        // edge 1. Port 20 stays = 5.
        g.run_pending().await.unwrap();
        assert_eq!(g.get_port_value(PortId::new(10)), Some(&Value::Int(5)));
        assert_eq!(g.get_port_value(PortId::new(20)), Some(&Value::Int(5)));
    }

    /// Three edges with completely disjoint ports — all land in wave 0 and
    /// execute concurrently.  This test validates correctness of the parallel
    /// path: every output must carry the right value even when transforms run
    /// simultaneously.
    #[tokio::test]
    async fn parallel_independent_edges_all_execute() {
        let mut g = Graph::new(GraphId::new(1), TransformRegistry::new());

        // Edge A: port 10 → port 20
        g.add_edge(HyperEdge::new(
            EdgeId::new(1),
            vec![PortId::new(10)],
            vec![PortId::new(20)],
            EdgeTransform::PassThrough,
            EdgeSchedule::OnChange,
        ))
        .unwrap();

        // Edge B: port 30 → port 40
        g.add_edge(HyperEdge::new(
            EdgeId::new(2),
            vec![PortId::new(30)],
            vec![PortId::new(40)],
            EdgeTransform::PassThrough,
            EdgeSchedule::OnChange,
        ))
        .unwrap();

        // Edge C: port 50 → port 60
        g.add_edge(HyperEdge::new(
            EdgeId::new(3),
            vec![PortId::new(50)],
            vec![PortId::new(60)],
            EdgeTransform::PassThrough,
            EdgeSchedule::OnChange,
        ))
        .unwrap();

        g.set_port_value(PortId::new(10), Value::Int(1));
        g.set_port_value(PortId::new(30), Value::Int(2));
        g.set_port_value(PortId::new(50), Value::Int(3));

        g.run_pending().await.unwrap();

        assert_eq!(g.get_port_value(PortId::new(20)), Some(&Value::Int(1)));
        assert_eq!(g.get_port_value(PortId::new(40)), Some(&Value::Int(2)));
        assert_eq!(g.get_port_value(PortId::new(60)), Some(&Value::Int(3)));
    }

    /// Two chained edges (A writes port 5, B reads port 5) must execute in
    /// separate waves even when both are pending at the same time.
    #[tokio::test]
    async fn chained_edges_execute_in_order() {
        let mut g = Graph::new(GraphId::new(1), TransformRegistry::new());

        // Edge A: port 1 → port 5
        g.add_edge(HyperEdge::new(
            EdgeId::new(1),
            vec![PortId::new(1)],
            vec![PortId::new(5)],
            EdgeTransform::PassThrough,
            EdgeSchedule::OnChange,
        ))
        .unwrap();

        // Edge B: port 5 → port 9
        g.add_edge(HyperEdge::new(
            EdgeId::new(2),
            vec![PortId::new(5)],
            vec![PortId::new(9)],
            EdgeTransform::PassThrough,
            EdgeSchedule::OnChange,
        ))
        .unwrap();

        g.set_port_value(PortId::new(1), Value::Int(42));
        // Manually mark edge 2 as pending too so both are drained together.
        g.scheduler.notify_change(PortId::new(5));

        g.run_pending().await.unwrap();

        // After wave 0 (edge A) writes port 5 = 42, wave 1 (edge B) reads
        // it and writes port 9 = 42.
        assert_eq!(g.get_port_value(PortId::new(9)), Some(&Value::Int(42)));
    }
}
