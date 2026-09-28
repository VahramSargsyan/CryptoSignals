from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from scripts.build_relative_rotation_dataset_cache import (
    TIMEFRAMES,
    _sha256,
    dataset_plan,
)
from strategies.crypto.relative_rotation.paper_live import ASSETS


class RelativeRotationDatasetCacheTests(unittest.TestCase):
    def test_plan_covers_every_asset_and_both_timeframes_once(self):
        plan = dataset_plan()
        self.assertEqual(len(plan), len(ASSETS) * len(TIMEFRAMES))
        self.assertEqual(len(set(plan)), len(plan))
        for asset in ASSETS:
            for timeframe in TIMEFRAMES:
                self.assertIn((asset, timeframe), plan)

    def test_plan_is_stable_asset_major_order(self):
        plan = dataset_plan()
        self.assertEqual(plan[0], (ASSETS[0], TIMEFRAMES[0]))
        self.assertEqual(plan[1], (ASSETS[0], TIMEFRAMES[1]))
        self.assertEqual(plan[-1], (ASSETS[-1], TIMEFRAMES[-1]))

    def test_sha256_matches_file_bytes(self):
        payload = b"canonical market data fixture\n"
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "fixture.csv"
            path.write_bytes(payload)
            self.assertEqual(_sha256(path), hashlib.sha256(payload).hexdigest())


if __name__ == "__main__":
    unittest.main()
