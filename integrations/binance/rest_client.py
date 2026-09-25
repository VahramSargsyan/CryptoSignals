from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd

SPOT_KLINES_PATH = "/api/v3/klines"
DEFAULT_BASE_URL = "https://data-api.binance.vision"
MAX_KLINES_PER_REQUEST = 1000


class BinancePublicApiError(RuntimeError):
    def __init__(self, message: str, *, status: Optional[int] = None, code: Any = None):
        super().__init__(message)
        self.status = status
        self.code = code


@dataclass(frozen=True)
class BinanceRestConfig:
    base_url: str = DEFAULT_BASE_URL
    timeout_seconds: float = 30.0
    page_limit: int = MAX_KLINES_PER_REQUEST
    max_pages: int = 1000

    def __post_init__(self) -> None:
        if not 1 <= self.page_limit <= MAX_KLINES_PER_REQUEST:
            raise ValueError("page_limit must be between 1 and 1000")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self.max_pages <= 0:
            raise ValueError("max_pages must be positive")


class BinanceSpotRestClient:
    """Minimal public Spot kline client compatible with HistoricalKlineClient."""

    def __init__(
        self,
        config: BinanceRestConfig | None = None,
        *,
        opener: Optional[Callable[..., Any]] = None,
    ):
        self.config = config or BinanceRestConfig()
        self._opener = opener or urlopen

    @staticmethod
    def _to_epoch_ms(value: Any) -> int:
        if isinstance(value, (int, float)):
            return int(value)
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        else:
            timestamp = timestamp.tz_convert("UTC")
        return int(timestamp.timestamp() * 1000)

    def _request_page(self, params: dict[str, Any]) -> list[list[Any]]:
        url = self.config.base_url.rstrip("/") + SPOT_KLINES_PATH + "?" + urlencode(params)
        try:
            with self._opener(url, timeout=self.config.timeout_seconds) as response:
                status = int(getattr(response, "status", 200))
                payload = response.read().decode("utf-8")
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(body)
                code = parsed.get("code")
                message = parsed.get("msg") or body
            except json.JSONDecodeError:
                code = None
                message = body
            raise BinancePublicApiError(
                f"Binance HTTP {exc.code}: {message}",
                status=exc.code,
                code=code,
            ) from exc
        except URLError as exc:
            raise BinancePublicApiError(f"Binance network error: {exc.reason}") from exc

        if status != 200:
            raise BinancePublicApiError(f"Binance HTTP {status}", status=status)

        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise BinancePublicApiError("Binance returned invalid JSON") from exc

        if isinstance(decoded, dict):
            raise BinancePublicApiError(
                f"Binance API error: {decoded.get('msg', decoded)}",
                status=status,
                code=decoded.get("code"),
            )
        if not isinstance(decoded, list):
            raise BinancePublicApiError("Binance klines response must be a list")
        return decoded

    def get_historical_klines(
        self,
        symbol: str,
        interval: str,
        start_str: Any,
        end_str: Any = None,
    ) -> list[list[Any]]:
        start_ms = self._to_epoch_ms(start_str)
        end_ms = self._to_epoch_ms(end_str) if end_str is not None else None
        if end_ms is not None and end_ms < start_ms:
            raise ValueError("end_str must be greater than or equal to start_str")

        rows: list[list[Any]] = []
        cursor = start_ms

        for _ in range(self.config.max_pages):
            params: dict[str, Any] = {
                "symbol": symbol.upper(),
                "interval": interval,
                "startTime": cursor,
                "limit": self.config.page_limit,
                "timeZone": "0",
            }
            if end_ms is not None:
                params["endTime"] = end_ms

            page = self._request_page(params)
            if not page:
                return rows

            first_open = int(page[0][0])
            last_open = int(page[-1][0])
            if last_open < cursor:
                raise BinancePublicApiError("Binance pagination moved backwards")
            if rows and first_open <= int(rows[-1][0]):
                raise BinancePublicApiError("Binance pagination returned overlapping pages")

            rows.extend(page)
            if len(page) < self.config.page_limit:
                return rows

            next_cursor = last_open + 1
            if next_cursor <= cursor:
                raise BinancePublicApiError("Binance pagination made no progress")
            if end_ms is not None and next_cursor > end_ms:
                return rows
            cursor = next_cursor

        raise BinancePublicApiError(
            f"Binance pagination exceeded max_pages={self.config.max_pages}"
        )
