from __future__ import annotations

import io
import json
import unittest
import zipfile

from scripts.research_official_liquidity_data_feasibility_v1 import inspect_h41, inspect_rrp


class OfficialLiquidityFeasibilityV1Tests(unittest.TestCase):
    def test_h41_inventory_resolves_explicit_series_tokens(self):
        data = """<Root>
          <Series SERIES_NAME="RESPPA_N.WW" UNIT="USD millions">
            <Obs TIME_PERIOD="2026-01-07" OBS_VALUE="6600000"/>
          </Series>
          <Series SERIES_NAME="TGA_BANK_1" COMPONENT="DEPUSTG" DISTRIBUTION="1" SERIESTYPE="L" CURRENCY="USD" UNIT="Currency" UNIT_MULT="1000000">
            <Obs TIME_PERIOD="2026-01-07" OBS_VALUE="100000"/>
          </Series>
          <Series SERIES_NAME="TGA_TEST" COMPONENT="DEPUSTG" DISTRIBUTION="TOT" SERIESTYPE="L" CURRENCY="USD" UNIT="Currency" UNIT_MULT="1000000">
            <Obs TIME_PERIOD="2026-01-07" OBS_VALUE="700000"/>
          </Series>
          <Series SERIES_NAME="TGA_AVG" COMPONENT="DEPUSTG" DISTRIBUTION="TOT" SERIESTYPE="A" CURRENCY="USD" UNIT="Currency" UNIT_MULT="1000000">
            <Obs TIME_PERIOD="2026-01-07" OBS_VALUE="650000"/>
          </Series>
        </Root>"""
        struct = """<Root><Code value="DEPUSTG"><Description>Deposits with FR Banks: U.S. Treasury, General Account</Description></Code></Root>"""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("H41_data.xml", data)
            zf.writestr("H41_struct.xml", struct)
        result = inspect_h41(buf.getvalue())
        self.assertEqual(len(result["total_assets_matches"]), 1)
        self.assertEqual(
            result["unique_tga_series_names_from_code_match"],
            ["TGA_AVG", "TGA_BANK_1", "TGA_TEST"],
        )
        self.assertEqual(result["tga_total_level_series_names"], ["TGA_TEST"])
        self.assertTrue(result["structure_tga_hits"])

    def test_rrp_shape(self):
        payload = {
            "repo": {
                "operations": [
                    {
                        "operationDate": "2026-01-07",
                        "operationType": "Reverse Repo",
                        "totalAmtAccepted": "1000000000",
                    }
                ]
            }
        }
        result = inspect_rrp(json.dumps(payload).encode())
        self.assertEqual(result["reverse_candidate_count"], 1)
        self.assertEqual(result["first_operation_date"], "2026-01-07")


if __name__ == "__main__":
    unittest.main()
