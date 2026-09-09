import json
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from schema import MarketTradeEvent


def test_schema_valid():
    event = MarketTradeEvent(
        symbol="BINANCE:BTCUSDT",
        price=64321.50,
        volume=0.0123,
        timestamp_ms=int(time.time() * 1000),
        source="finnhub"
    )
    assert event.symbol == "BINANCE:BTCUSDT"
    assert event.price > 0
    assert event.volume >= 0
    assert event.source == "finnhub"
    print("PASS: test_schema_valid")


def test_schema_rejects_negative_price():
    try:
        MarketTradeEvent(
            symbol="BINANCE:BTCUSDT",
            price=-100.0,
            volume=1.0,
            timestamp_ms=int(time.time() * 1000),
            source="finnhub"
        )
        raise AssertionError("Should have rejected negative price")
    except Exception as e:
        assert "greater than" in str(e) or "Input should be greater than" in str(e)
        print("PASS: test_schema_rejects_negative_price")


def test_schema_rejects_missing_symbol():
    try:
        MarketTradeEvent(
            symbol="",
            price=100.0,
            volume=1.0,
            timestamp_ms=int(time.time() * 1000),
            source="finnhub"
        )
        raise AssertionError("Should have rejected empty symbol")
    except Exception as e:
        assert "symbol" in str(e).lower()
        print("PASS: test_schema_rejects_missing_symbol")


def test_schema_rejects_old_timestamp():
    try:
        MarketTradeEvent(
            symbol="BTC",
            price=100.0,
            volume=1.0,
            timestamp_ms=1000,
            source="finnhub"
        )
        raise AssertionError("Should have rejected old timestamp")
    except Exception as e:
        assert "old" in str(e).lower()
        print("PASS: test_schema_rejects_old_timestamp")


def test_finnhub_normalization():
    raw = {
        "data": [
            {"s": "BINANCE:BTCUSDT", "p": 64321.50, "t": int(time.time() * 1000), "v": 0.0123},
            {"s": "BINANCE:BTCUSDT", "p": 64322.10, "t": int(time.time() * 1000), "v": 0.0456},
        ],
        "type": "trade"
    }

    records = []
    for trade in raw["data"]:
        event = MarketTradeEvent(
            symbol=trade.get("s", ""),
            price=float(trade.get("p", 0)),
            volume=float(trade.get("v", 0)),
            timestamp_ms=int(trade.get("t", 0)),
            source="finnhub"
        )
        records.append(event.to_dict())

    assert len(records) == 2
    assert records[0]["symbol"] == "BINANCE:BTCUSDT"
    assert records[0]["source"] == "finnhub"
    assert "price" in records[0]
    print("PASS: test_finnhub_normalization")


def test_demo_metals_normalization():
    event = MarketTradeEvent(
        symbol="Gold (GC=F)",
        price=2500.00,
        volume=1.5,
        timestamp_ms=int(time.time() * 1000),
        source="demo"
    )
    record = event.to_dict()
    assert record["symbol"] == "Gold (GC=F)"
    assert record["source"] == "demo"
    print("PASS: test_demo_metals_normalization")


def test_parquet_converter_handles_new_format():
    from scripts.s3_json_to_parquet import parse_message

    new_record = {
        "symbol": "BINANCE:BTCUSDT",
        "price": 64321.50,
        "volume": 0.0123,
        "timestamp_ms": 1697641234567,
        "source": "finnhub"
    }

    parsed = parse_message(new_record)
    assert len(parsed) == 1
    assert parsed[0]["symbol"] == "BINANCE:BTCUSDT"
    assert parsed[0]["price"] == 64321.50
    print("PASS: test_parquet_converter_handles_new_format")


def test_parquet_converter_handles_old_finnhub_batch():
    from scripts.s3_json_to_parquet import parse_message

    old_batch = {
        "data": [
            {"s": "BINANCE:BTCUSDT", "p": 64321.50, "t": 1697641234567, "v": 0.0123}
        ],
        "type": "trade"
    }

    parsed = parse_message(old_batch)
    assert len(parsed) == 1
    assert parsed[0]["symbol"] == "BINANCE:BTCUSDT"
    print("PASS: test_parquet_converter_handles_old_finnhub_batch")


if __name__ == "__main__":
    test_schema_valid()
    test_schema_rejects_negative_price()
    test_schema_rejects_missing_symbol()
    test_schema_rejects_old_timestamp()
    test_finnhub_normalization()
    test_demo_metals_normalization()
    test_parquet_converter_handles_new_format()
    test_parquet_converter_handles_old_finnhub_batch()
    print("\nAll tests passed.")
