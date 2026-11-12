# Module 2: Data Lifecycle, Ingestion, and Processing

This module covers the stages of the data lifecycle, ingestion strategies (batch vs. real-time/streaming), and different data processing paradigms, frameworks, and patterns.

## Exercises

*   [**Exercise 1: Change Data Capture (CDC) with Debezium Server and PostgreSQL**](./exercise-1-cdc-debezium.md)
    *   Deploy a local instance of Debezium Server using Docker Compose to capture database mutations (CDC) and stream them into a file sink.
    *   Solution: [`debezium-test/`](./debezium-test/README.md)
*   [**Exercise 2: End-to-End Data Flow Pipeline**](./exercise-2-end-to-end-pipeline.md)
    *   Build and deploy a functional end-to-end data pipeline that ingests, processes, stores, and exposes a dataset via a REST or gRPC API.
    *   Solution: [`end-to-end-pipeline/`](./end-to-end-pipeline/README.md)

## Licencia

MIT, ver [LICENSE](LICENSE).

---
[⬅ Back to Home](../README.md)
