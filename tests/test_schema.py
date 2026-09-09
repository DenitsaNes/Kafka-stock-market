import time

import pytest

from schema import MarketTradeEvent


def test_valid_event():
    event = MarketTradeEvent(
        symbol="BINANCE:BTCUSDT",
        price=64321.50,
        volume=0.0123,
        timestamp_ms=int(time.time() * 1000),
        source="finnhub"
    )
    assert event.symbol == "BINANCE:BTCUSDT"
    assert event.price == 64321.50
    assert event.volume == 0.0123
    assert event.source == "finnhub"


def test_rejects_negative_price():
    with pytest.raises(ValueError):
        MarketTradeEvent(
            symbol="BTC",
            price=-100.0,
            volume=1.0,
            timestamp_ms=int(time.time() * 1000),
            source="finnhub"
        )


def test_rejects_zero_price():
    with pytest.raises(ValueError):
        MarketTradeEvent(
            symbol="BTC",
            price=0.0,
            volume=1.0,
            timestamp_ms=int(time.time() * 1000),
            source="finnhub"
        )


def test_rejects_negative_volume():
    with pytest.raises(ValueError):
        MarketTradeEvent(
            symbol="BTC",
            price=100.0,
            volume=-1.0,
            timestamp_ms=int(time.time() * 1000),
            source="finnhub"
        )


def test_rejects_missing_symbol():
    with pytest.raises(ValueError):
        MarketTradeEvent(
            symbol="",
            price=100.0,
            volume=1.0,
            timestamp_ms=int(time.time() * 1000),
            source="finnhub"
        )


def test_rejects_old_timestamp():
    with pytest.raises(ValueError):
        MarketTradeEvent(
            symbol="BTC",
            price=100.0,
            volume=1.0,
            timestamp_ms=1000,
            source="finnhub"
        )


def test_rejects_future_timestamp():
    future = int(time.time() * 1000) + 3_700_000  # more than 1 hour ahead
    with pytest.raises(ValueError):
        MarketTradeEvent(
            symbol="BTC",
            price=100.0,
            volume=1.0,
            timestamp_ms=future,
            source="finnhub"
        )


def test_to_dict_returns_plain_dict():
    event = MarketTradeEvent(
        symbol="BTC",
        price=100.0,
        volume=1.0,
        timestamp_ms=int(time.time() * 1000),
        source="finnhub"
    )
    record = event.to_dict()
    assert isinstance(record, dict)
    assert record["symbol"] == "BTC"
    assert record["source"] == "finnhub"
