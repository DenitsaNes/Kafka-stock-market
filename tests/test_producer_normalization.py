import time

from schema import MarketTradeEvent


def normalize_finnhub_trade(trade):
    event = MarketTradeEvent(
        symbol=trade.get("s", ""),
        price=float(trade.get("p", 0)),
        volume=float(trade.get("v", 0)),
        timestamp_ms=int(trade.get("t", 0)),
        source="finnhub"
    )
    return event.to_dict()


def test_normalize_finnhub_trade():
    trade = {"s": "BINANCE:BTCUSDT", "p": 64321.50, "t": 1697641234567, "v": 0.0123}
    record = normalize_finnhub_trade(trade)

    assert record["symbol"] == "BINANCE:BTCUSDT"
    assert record["price"] == 64321.50
    assert record["volume"] == 0.0123
    assert record["timestamp_ms"] == 1697641234567
    assert record["source"] == "finnhub"


def test_normalize_demo_metals():
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


def test_invalid_trade_is_skipped():
    trade = {"s": "", "p": -100.0, "t": 0, "v": 0}
    try:
        normalize_finnhub_trade(trade)
        raise AssertionError("Should have raised ValueError")
    except Exception as e:
        assert "symbol" in str(e).lower() or "greater than" in str(e).lower()
