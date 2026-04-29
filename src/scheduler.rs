//! Edge scheduler — tracks which edges should fire and when.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use loom_domain::{EdgeSchedule, HyperEdge};
use plexus_base::{EdgeId, PortId};

/// Tracks scheduling state for all registered edges and accumulates a queue of
/// edge ids that are ready to execute.
#[derive(Default)]
pub struct Scheduler {
    /// Edges that fire whenever one of their source ports changes.
    on_change_edges: Vec<(EdgeId, Vec<PortId>)>,
    /// Edges that fire on a timer: `(id, interval, elapsed_since_last_fire)`.
    on_tick_edges: Vec<(EdgeId, Duration, Duration)>,
    /// Edges that fire on a named event.
    on_event_edges: HashMap<String, Vec<EdgeId>>,
    /// Queue of edge ids ready to run (may contain duplicates; deduped on drain).
    pending: VecDeque<EdgeId>,
}

impl Scheduler {
    /// Create an empty scheduler.
    pub fn new() -> Self {
        Scheduler::default()
    }

    /// Register an edge with the scheduler.
    ///
    /// The scheduling policy is taken from the edge's [`EdgeSchedule`].
    pub fn register(&mut self, edge: &HyperEdge) {
        match &edge.schedule {
            EdgeSchedule::OnChange => {
                self.on_change_edges
                    .push((edge.id, edge.sources.clone()));
            }
            EdgeSchedule::OnTick(interval) => {
                self.on_tick_edges.push((edge.id, *interval, Duration::ZERO));
            }
            EdgeSchedule::OnEvent(name) => {
                self.on_event_edges
                    .entry(name.clone())
                    .or_default()
                    .push(edge.id);
            }
        }
    }

    /// Notify the scheduler that `port` has a new value.
    ///
    /// All `OnChange` edges whose source list includes `port` are enqueued.
    pub fn notify_change(&mut self, port: PortId) {
        let pending = &mut self.pending;
        for (id, sources) in &self.on_change_edges {
            if sources.contains(&port) {
                pending.push_back(*id);
            }
        }
    }

    /// Advance time by `delta`.  All `OnTick` edges whose interval has elapsed
    /// are enqueued (and their elapsed counter resets to zero).
    pub fn tick(&mut self, delta: Duration) {
        for (id, interval, elapsed) in &mut self.on_tick_edges {
            *elapsed += delta;
            while *elapsed >= *interval {
                *elapsed -= *interval;
                self.pending.push_back(*id);
            }
        }
    }

    /// Fire a named event, enqueuing all edges registered for that event name.
    pub fn fire_event(&mut self, name: &str) {
        if let Some(ids) = self.on_event_edges.get(name) {
            for &id in ids {
                self.pending.push_back(id);
            }
        }
    }

    /// Drain the pending queue, returning edge ids in FIFO order.
    ///
    /// Duplicate ids are deduplicated while preserving first-occurrence order.
    pub fn drain_pending(&mut self) -> Vec<EdgeId> {
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        while let Some(id) = self.pending.pop_front() {
            if seen.insert(id) {
                result.push(id);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loom_domain::{EdgeSchedule, EdgeTransform, HyperEdge};
    use plexus_base::{EdgeId, PortId};

    fn on_change_edge(id: u64, sources: &[u64]) -> HyperEdge {
        HyperEdge::new(
            EdgeId::new(id),
            sources.iter().map(|&p| PortId::new(p)).collect(),
            vec![],
            EdgeTransform::PassThrough,
            EdgeSchedule::OnChange,
        )
    }

    fn on_tick_edge(id: u64, ms: u64) -> HyperEdge {
        HyperEdge::new(
            EdgeId::new(id),
            vec![],
            vec![],
            EdgeTransform::PassThrough,
            EdgeSchedule::OnTick(Duration::from_millis(ms)),
        )
    }

    fn on_event_edge(id: u64, name: &str) -> HyperEdge {
        HyperEdge::new(
            EdgeId::new(id),
            vec![],
            vec![],
            EdgeTransform::PassThrough,
            EdgeSchedule::OnEvent(name.to_string()),
        )
    }

    #[test]
    fn on_change_fires_when_source_changes() {
        let mut sched = Scheduler::new();
        sched.register(&on_change_edge(1, &[10, 20]));

        sched.notify_change(PortId::new(10));
        let pending = sched.drain_pending();
        assert_eq!(pending, vec![EdgeId::new(1)]);
    }

    #[test]
    fn on_change_does_not_fire_for_unrelated_port() {
        let mut sched = Scheduler::new();
        sched.register(&on_change_edge(1, &[10]));

        sched.notify_change(PortId::new(99));
        assert!(sched.drain_pending().is_empty());
    }

    #[test]
    fn on_tick_fires_after_interval() {
        let mut sched = Scheduler::new();
        sched.register(&on_tick_edge(2, 100));

        sched.tick(Duration::from_millis(50));
        assert!(sched.drain_pending().is_empty());

        sched.tick(Duration::from_millis(60));
        let pending = sched.drain_pending();
        assert_eq!(pending, vec![EdgeId::new(2)]);
    }

    #[test]
    fn on_tick_fires_multiple_times_if_overshot() {
        let mut sched = Scheduler::new();
        sched.register(&on_tick_edge(3, 100));

        sched.tick(Duration::from_millis(250));
        let pending = sched.drain_pending();
        // The edge is enqueued twice (250ms / 100ms = 2 intervals) but
        // drain_pending deduplicates, so it appears once in the result.
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0], EdgeId::new(3));
    }

    #[test]
    fn on_event_fires_on_matching_name() {
        let mut sched = Scheduler::new();
        sched.register(&on_event_edge(4, "click"));

        sched.fire_event("click");
        let pending = sched.drain_pending();
        assert_eq!(pending, vec![EdgeId::new(4)]);
    }

    #[test]
    fn on_event_does_not_fire_for_wrong_name() {
        let mut sched = Scheduler::new();
        sched.register(&on_event_edge(4, "click"));

        sched.fire_event("hover");
        assert!(sched.drain_pending().is_empty());
    }

    #[test]
    fn drain_deduplicates() {
        let mut sched = Scheduler::new();
        sched.register(&on_change_edge(1, &[10]));

        sched.notify_change(PortId::new(10));
        sched.notify_change(PortId::new(10));
        let pending = sched.drain_pending();
        assert_eq!(pending, vec![EdgeId::new(1)]);
    }
}
