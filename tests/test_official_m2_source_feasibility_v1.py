from __future__ import annotations

import io
import unittest
import zipfile

from scripts.research_official_m2_source_feasibility_v1 import inspect_h6_m2


class OfficialM2SourceFeasibilityV1Tests(unittest.TestCase):
    def _zip(self, series_name: str = "M2.M") -> bytes:
        xml = f'''<?xml version="1.0"?>
        <Root>
          <Series SERIES_NAME="{series_name}" CURRENCY="USD" UNIT_MULT="1000000000" FREQ="M">
            <Obs TIME_PERIOD="2022-01" OBS_VALUE="21000.0"/>
            <Obs TIME_PERIOD="2022-02" OBS_VALUE="21100.0"/>
          </Series>
        </Root>'''.encode()
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("H6_data.xml", xml)
        return buf.getvalue()

    def test_extracts_unique_m2_monthly_series(self):
        frame, meta = inspect_h6_m2(self._zip())
        self.assertEqual(meta["series_name"], "M2.M")
        self.assertEqual(len(frame), 2)
        self.assertEqual(frame.iloc[0]["period"], "2022-01")

    def test_rejects_missing_target_series(self):
        with self.assertRaises(RuntimeError):
            inspect_h6_m2(self._zip("M1.M"))


if __name__ == "__main__":
    unittest.main()
