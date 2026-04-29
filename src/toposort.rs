//! Topological sort (Kahn's algorithm) over a set of [`HyperEdge`]s.
//!
//! Edges are treated as nodes.  An edge B depends on edge A if any output port
//! of A appears as an input (source) port of B.

use std::collections::{HashMap, HashSet, VecDeque};

use frp_domain::HyperEdge;
use frp_plexus::EdgeId;

use crate::error::EngineError;

/// Topologically sort a slice of [`HyperEdge`]s.
///
/// Returns a `Vec<EdgeId>` in execution order — i.e. each edge's inputs are
/// produced by edges that appear earlier in the list.
///
/// Returns `EngineError::CycleDetected` if the edge dependency graph contains
/// a cycle.
pub fn toposort(edges: &[HyperEdge]) -> Result<Vec<EdgeId>, EngineError> {
    // Build adjacency: edge_id -> set of edge_ids that depend on it.
    // Also count in-degrees.
    let ids: Vec<EdgeId> = edges.iter().map(|e| e.id).collect();

    // Map from output PortId -> EdgeId that produces it.
    let mut port_producer: HashMap<frp_plexus::PortId, EdgeId> = HashMap::new();
    for edge in edges {
        for &port in &edge.targets {
            port_producer.insert(port, edge.id);
        }
    }

    // adj[a] = edges that must come AFTER a (a produces something b needs).
    let mut adj: HashMap<EdgeId, Vec<EdgeId>> = HashMap::new();
    let mut in_degree: HashMap<EdgeId, usize> = HashMap::new();
    for id in &ids {
        adj.entry(*id).or_default();
        in_degree.entry(*id).or_insert(0);
    }

    for edge in edges {
        let mut seen_deps: HashSet<EdgeId> = HashSet::new();
        for &src_port in &edge.sources {
            if let Some(&producer) = port_producer.get(&src_port) {
                if producer != edge.id && seen_deps.insert(producer) {
                    adj.entry(producer).or_default().push(edge.id);
                    *in_degree.entry(edge.id).or_insert(0) += 1;
                }
            }
        }
    }

    // Kahn's algorithm.
    let mut queue: VecDeque<EdgeId> = in_degree
        .iter()
        .filter(|&(_, deg)| *deg == 0)
        .map(|(&id, _)| id)
        .collect();

    let mut sorted = Vec::with_capacity(ids.len());

    while let Some(id) = queue.pop_front() {
        sorted.push(id);
        if let Some(dependents) = adj.get(&id) {
            for &dep in dependents {
                let deg = in_degree.entry(dep).or_insert(0);
                *deg -= 1;
                if *deg == 0 {
                    queue.push_back(dep);
                }
            }
        }
    }

    if sorted.len() != ids.len() {
        return Err(EngineError::CycleDetected);
    }

    Ok(sorted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use frp_domain::{EdgeSchedule, EdgeTransform, HyperEdge};
    use frp_plexus::{EdgeId, PortId};

    fn edge(id: u64, sources: &[u64], targets: &[u64]) -> HyperEdge {
        HyperEdge::new(
            EdgeId::new(id),
            sources.iter().map(|&p| PortId::new(p)).collect(),
            targets.iter().map(|&p| PortId::new(p)).collect(),
            EdgeTransform::PassThrough,
            EdgeSchedule::OnChange,
        )
    }

    #[test]
    fn single_edge_sorted() {
        let edges = vec![edge(1, &[10], &[20])];
        let sorted = toposort(&edges).unwrap();
        assert_eq!(sorted, vec![EdgeId::new(1)]);
    }

    #[test]
    fn two_independent_edges() {
        let edges = vec![edge(1, &[10], &[20]), edge(2, &[30], &[40])];
        let sorted = toposort(&edges).unwrap();
        assert_eq!(sorted.len(), 2);
    }

    #[test]
    fn chain_a_produces_b_input() {
        // edge 1: [] -> [port 5]
        // edge 2: [port 5] -> [port 6]   (depends on edge 1)
        let edges = vec![edge(2, &[5], &[6]), edge(1, &[], &[5])];
        let sorted = toposort(&edges).unwrap();
        let pos_1 = sorted.iter().position(|&id| id == EdgeId::new(1)).unwrap();
        let pos_2 = sorted.iter().position(|&id| id == EdgeId::new(2)).unwrap();
        assert!(pos_1 < pos_2, "edge 1 must come before edge 2");
    }

    #[test]
    fn cycle_returns_error() {
        // edge 1: [port 2] -> [port 1]
        // edge 2: [port 1] -> [port 2]
        let edges = vec![edge(1, &[2], &[1]), edge(2, &[1], &[2])];
        let err = toposort(&edges).unwrap_err();
        assert!(matches!(err, EngineError::CycleDetected));
    }

    #[test]
    fn empty_edges_ok() {
        let sorted = toposort(&[]).unwrap();
        assert!(sorted.is_empty());
    }
}
