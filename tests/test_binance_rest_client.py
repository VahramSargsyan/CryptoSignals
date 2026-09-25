import io
import json
import unittest
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse

from integrations.binance.rest_client import (
    BinancePublicApiError,
    BinanceRestConfig,
    BinanceSpotRestClient,
)


class FakeResponse:
    def __init__(self, payload, status=200):
        self.payload = json.dumps(payload).encode("utf-8")
        self.status = status

    def read(self):
        return self.payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class BinanceRestClientTests(unittest.TestCase):
    def test_pages_forward_without_overlap(self):
        calls = []
        pages = [
            [[1000, "1", "2", "0.5", "1.5", "10"], [2000, "1.5", "2", "1", "1.8", "12"]],
            [[3000, "1.8", "2", "1.5", "1.9", "13"]],
        ]

        def opener(url, timeout):
            calls.append((url, timeout))
            return FakeResponse(pages[len(calls) - 1])

        client = BinanceSpotRestClient(
            BinanceRestConfig(page_limit=2),
            opener=opener,
        )
        rows = client.get_historical_klines("btcusdt", "1d", 1000, 5000)
        self.assertEqual([row[0] for row in rows], [1000, 2000, 3000])
        first_query = parse_qs(urlparse(calls[0][0]).query)
        second_query = parse_qs(urlparse(calls[1][0]).query)
        self.assertEqual(first_query["symbol"], ["BTCUSDT"])
        self.assertEqual(first_query["timeZone"], ["0"])
        self.assertEqual(second_query["startTime"], ["2001"])
        self.assertTrue(calls[0][0].startswith("https://data-api.binance.vision/api/v3/klines?"))

    def test_http_api_error_is_normalized(self):
        def opener(url, timeout):
            body = io.BytesIO(json.dumps({"code": -1121, "msg": "Invalid symbol."}).encode("utf-8"))
            raise HTTPError(url, 400, "Bad Request", hdrs=None, fp=body)

        client = BinanceSpotRestClient(opener=opener)
        with self.assertRaises(BinancePublicApiError) as caught:
            client.get_historical_klines("BADUSDT", "1d", 1000, 2000)
        self.assertEqual(caught.exception.status, 400)
        self.assertEqual(caught.exception.code, -1121)
        self.assertIn("Invalid symbol", str(caught.exception))

    def test_overlapping_pages_fail_closed(self):
        pages = [
            [[1000, "1", "2", "0.5", "1.5", "10"], [2000, "1.5", "2", "1", "1.8", "12"]],
            [[2000, "1.5", "2", "1", "1.8", "12"], [3000, "1.8", "2", "1.5", "1.9", "13"]],
        ]
        count = {"value": 0}

        def opener(url, timeout):
            index = count["value"]
            count["value"] += 1
            return FakeResponse(pages[index])

        client = BinanceSpotRestClient(
            BinanceRestConfig(page_limit=2),
            opener=opener,
        )
        with self.assertRaises(BinancePublicApiError):
            client.get_historical_klines("BTCUSDT", "1d", 1000, 5000)


if __name__ == "__main__":
    unittest.main()
