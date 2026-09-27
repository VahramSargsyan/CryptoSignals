from __future__ import annotations

import unittest

from scripts.research_relative_rotation_graph_intelligence import (
    ASSETS,
    PairSignal,
    choose_candidate,
    graph_snapshot,
)


class GraphIntelligenceTests(unittest.TestCase):
    def _transitive_graph(self):
        comparisons = []
        for i, left in enumerate(ASSETS):
            for right in ASSETS[i + 1 :]:
                comparisons.append((left, right, left, 1.0))
        return graph_snapshot(comparisons)

    def test_transitive_tournament_has_no_cycles(self):
        graph = self._transitive_graph()
        self.assertEqual(graph["cyclic_triples"], 0)
        self.assertEqual(graph["cycle_ratio"], 0.0)

    def test_all_rankers_put_dominant_node_first(self):
        graph = self._transitive_graph()
        for method in ("BRADLEY_TERRY", "PAGERANK", "RANK_CENTRALITY"):
            self.assertEqual(graph["ranks"][method]["ATOM"], 1)

    def test_single_candidate_is_never_overridden(self):
        graph = self._transitive_graph()
        candidate = PairSignal(
            date=None,  # type: ignore[arg-type]
            from_asset="TWT",
            to_asset="LINK",
            strength=0.25,
            pair="TWT/LINK",
        )
        for method in ("BASELINE", "BRADLEY_TERRY", "PAGERANK", "RANK_CENTRALITY", "CONSENSUS_3"):
            self.assertEqual(choose_candidate([candidate], method, graph), candidate)

    def test_graph_rank_can_change_only_conflict_resolution(self):
        graph = self._transitive_graph()
        candidates = [
            PairSignal(
                date=None,  # type: ignore[arg-type]
                from_asset="LINK",
                to_asset="ATOM",
                strength=0.20,
                pair="ATOM/LINK",
            ),
            PairSignal(
                date=None,  # type: ignore[arg-type]
                from_asset="LINK",
                to_asset="TWT",
                strength=0.30,
                pair="TWT/LINK",
            ),
        ]
        self.assertEqual(choose_candidate(candidates, "BASELINE", graph).to_asset, "TWT")
        self.assertEqual(choose_candidate(candidates, "BRADLEY_TERRY", graph).to_asset, "ATOM")
        self.assertEqual(choose_candidate(candidates, "PAGERANK", graph).to_asset, "ATOM")
        self.assertEqual(choose_candidate(candidates, "RANK_CENTRALITY", graph).to_asset, "ATOM")
        self.assertEqual(choose_candidate(candidates, "CONSENSUS_3", graph).to_asset, "ATOM")


if __name__ == "__main__":
    unittest.main()
