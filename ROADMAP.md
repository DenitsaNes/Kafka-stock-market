# Kafka Stock Market — Improvement Roadmap

Status: **Portfolio-ready**. The end-to-end pipeline (Finnhub → Kafka → S3 → Parquet → Athena → Streamlit dashboard) is working and documented.

This roadmap lists the next improvements that will take the project from "good learning project" to a polished junior data-engineering portfolio piece.

---

## Phase A: Polish and publish (1–2 days)

1. **Add screenshots to README**
   - Save dashboard, dashboard2, athena, athena_ranking, athena_volume, and athena_price_change images under `assets/`.
   - Commit and push them so the GitHub README renders visuals immediately.

2. **Clean README wording**
   - Clarify that the current setup is a single-node development deployment on EC2.
   - Avoid implying AWS Free Tier is guaranteed.
   - Optionally rename the project to **Real-Time Market Data Platform** for stronger impact.

---

## Phase B: Streaming hardening (2–3 days)

3. **Kafka partitions and consumer groups**
   - Use multiple partitions for the trades topic (e.g., partition by symbol hash).
   - Assign separate consumer groups to the S3 sink and the dashboard.
   - This demonstrates Kafka scalability and decoupled consumers.

4. **Message validation and schema**
   - Define a formal `MarketTradeEvent` schema (symbol, price, volume, timestamp_ms, source).
   - Validate in the producer and consumer using Pydantic or JSON Schema.
   - Reject records with negative prices, missing symbols, or invalid timestamps.

5. **Error handling, retries, and dead-letter topic**
   - Add retry logic for Kafka reconnections and S3 upload failures.
   - Route invalid messages to a `market.trades.dlq` topic for investigation.
   - Replace `print()` with structured logging.

---

## Phase C: Quality and CI (1–2 days)

6. **Automated tests**
   - Test message parsing and schema validation.
   - Test S3 key generation and Parquet conversion output.
   - Test invalid inputs produce expected errors.

7. **GitHub Actions CI**
   - Run `pytest` on every push to `main`.
   - Ensure code changes do not break the pipeline logic.

---

## Phase D: Production architecture documentation (1 day)

8. **Document the production evolution** ✅
   - Explain current architecture: single-node Kafka + Zookeeper on EC2.
   - Explain production target: Amazon MSK with replicated brokers, monitoring, and consumer groups.
   - Do not migrate to MSK unless you specifically want to spend time on AWS managed services.

---

## Nice-to-have (after the above)

- Kafka metrics and Grafana dashboards
- Docker Compose for local development
- Schema Registry (Confluent or AWS Glue Schema Registry)
- Kafka Connect S3 Sink instead of the custom Python consumer

## Intentionally out of scope

These would distract from the core story or require infrastructure beyond the project scope:

- Apache Spark
- Apache Airflow
- Kubernetes / Terraform
- Machine learning / trading algorithms
- Microservices architecture

---

## Suggested execution order

1. Phase A — publish the current working version with screenshots.
2. Phase B — add partitions, validation, and DLQ.
3. Phase C — add tests and CI.
4. Phase D — document production architecture ✅.

This keeps each phase focused and gives you a stronger portfolio after every step.
