from datetime import datetime, timezone
from typing import Literal
from pydantic import BaseModel, Field, field_validator


class MarketTradeEvent(BaseModel):
    symbol: str = Field(..., min_length=1, description="Trading symbol, e.g. BINANCE:BTCUSDT")
    price: float = Field(..., gt=0, description="Trade price, must be positive")
    volume: float = Field(..., ge=0, description="Trade volume, must be non-negative")
    timestamp_ms: int = Field(..., gt=0, description="Unix timestamp in milliseconds")
    source: Literal["finnhub", "demo"] = Field(..., description="Origin of the trade record")

    @field_validator("symbol")
    @classmethod
    def symbol_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("symbol must not be empty")
        return v

    @field_validator("timestamp_ms")
    @classmethod
    def timestamp_must_be_reasonable(cls, v: int) -> int:
        # Reject timestamps before 2020-01-01 or more than 1 hour in the future.
        min_ts = 1_577_836_800_000  # 2020-01-01 00:00:00 UTC in ms
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        if v < min_ts:
            raise ValueError("timestamp is too old (before 2020)")
        if v > now_ms + 3_600_000:
            raise ValueError("timestamp is more than 1 hour in the future")
        return v

    def to_dict(self) -> dict:
        return self.model_dump()
