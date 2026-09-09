import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.s3_json_to_parquet import parse_message


def test_parse_new_format():
    record = {
        "symbol": "BINANCE:BTCUSDT",
        "price": 64321.50,
        "volume": 0.0123,
        "timestamp_ms": 1697641234567,
        "source": "finnhub"
    }

    parsed = parse_message(record)
    assert len(parsed) == 1
    assert parsed[0]["symbol"] == "BINANCE:BTCUSDT"
    assert parsed[0]["price"] == 64321.50
    assert parsed[0]["source"] == "finnhub"


def test_parse_legacy_finnhub_batch():
    batch = {
        "data": [
            {"s": "BINANCE:BTCUSDT", "p": 64321.50, "t": 1697641234567, "v": 0.0123}
        ],
        "type": "trade"
    }

    parsed = parse_message(batch)
    assert len(parsed) == 1
    assert parsed[0]["symbol"] == "BINANCE:BTCUSDT"
    assert parsed[0]["source"] == "finnhub"


def test_parse_legacy_demo_format():
    record = {"s": "Gold (GC=F)", "p": 2500.00, "t": 1697641234567, "v": 1.5}

    parsed = parse_message(record)
    assert len(parsed) == 1
    assert parsed[0]["symbol"] == "Gold (GC=F)"
    assert parsed[0]["source"] == "demo"
