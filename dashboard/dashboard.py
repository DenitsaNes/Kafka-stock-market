import json
import logging
import os
import time
from collections import deque, defaultdict
import streamlit as st
from kafka import KafkaConsumer
import plotly.graph_objects as go

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Crypto & Metals Dashboard", layout="wide")
st.title("₿💰 Real-time Crypto & Metals Dashboard")
st.caption("Live data from Kafka • updates every 5 seconds")

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'market.trades.raw')

metrics_placeholder = st.empty()
charts_placeholder = st.empty()

prices_by_symbol = defaultdict(lambda: deque(maxlen=200))

def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=1000,
        auto_offset_reset='latest',
        group_id='dashboard-consumer-group'
    )


consumer = create_consumer()

render_counter = 0
last_render_time = time.time()
RENDER_INTERVAL = 5.0

while True:
    try:
        messages = consumer.poll(timeout_ms=1000)
    except Exception as e:
        logger.error(f"Kafka consumer error: {e}")
        logger.info("Recreating Kafka consumer in 5s...")
        time.sleep(5)
        try:
            consumer.close()
        except Exception:
            pass
        consumer = create_consumer()
        continue

    got_data = False
    if messages:
        for topic_partition, msgs in messages.items():
            for msg in msgs:
                message = msg.value

                # All messages are now normalized MarketTradeEvent records.
                try:
                    symbol = message.get('symbol', 'UNKNOWN')
                    price = float(message.get('price', 0))
                    if price:
                        prices_by_symbol[symbol].append(price)
                        got_data = True
                except Exception as e:
                    logger.warning(f"Skipping malformed message: {message} — {e}")

    now = time.time()
    should_render = got_data and (now - last_render_time >= RENDER_INTERVAL)

    if should_render:
        render_counter += 1
        last_render_time = now

        metrics_placeholder.empty()
        charts_placeholder.empty()

        with metrics_placeholder.container():
            symbols = list(prices_by_symbol.keys())
            cols = st.columns(min(len(symbols), 5))
            for i, (symbol, prices) in enumerate(prices_by_symbol.items()):
                if len(prices) > 0:
                    latest = prices[-1]
                    col_idx = i % len(cols)
                    if len(prices) > 1:
                        prev = prices[-2]
                        change = latest - prev
                        change_pct = (change / prev) * 100
                        cols[col_idx].metric(
                            label=symbol,
                            value=f"${latest:,.2f}",
                            delta=f"{change:+.3f} ({change_pct:+.4f}%)"
                        )
                    else:
                        cols[col_idx].metric(
                            label=symbol,
                            value=f"${latest:,.2f}"
                        )

        with charts_placeholder.container():
            for i, (symbol, prices) in enumerate(prices_by_symbol.items()):
                if len(prices) > 1:
                    price_list = list(prices)
                    min_price = min(price_list)
                    max_price = max(price_list)
                    padding = (max_price - min_price) * 0.2 if max_price != min_price else max_price * 0.05

                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        y=price_list,
                        mode='lines',
                        name=symbol,
                        line=dict(width=2)
                    ))

                    fig.update_layout(
                        title=f"{symbol} Price Movement",
                        xaxis_title="Time",
                        yaxis_title="Price (USD)",
                        yaxis=dict(
                            range=[min_price - padding, max_price + padding],
                            tickformat=',.2f'
                        ),
                        template="plotly_dark",
                        height=300,
                        margin=dict(l=80, r=50, t=50, b=50)
                    )

                    st.plotly_chart(
                        fig,
                        use_container_width=True,
                        key=f"chart_{symbol}_{render_counter}_{i}"
                    )

    time.sleep(0.1)
