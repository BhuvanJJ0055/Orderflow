# Phase 5 Verification Report: Real-Time Kafka Streaming Pipeline

- **Operating System:** Windows 10
- **CPU Count:** 12
- **Python Version:** 3.11.6
- **Verification Timestamp:** 2026-10-30 17:38:25

| Check | Result | Evidence |
| --- | --- | --- |
| Broker & Topic Verification | `PASS` | Broker active at localhost:9092; topic 'orders' exists (3 partitions) |
| Fresh Streaming Ingestion | `PASS` | Expected valid=282, Saved rows=282, Duplicates=0, Invalid amounts (<=0)=0 |
| Consumer Crash & Resume Resilience | `PASS` | Pre-crash=282, Added valid=282, Final total=564, Duplicate events=0 (zero loss, zero duplicate replay) |
| Ingestion Latency (Avg & p95) | `PASS` | Average latency: 12182.0ms, p95 latency: 42435.0ms (measured across all ingested live events on local single-broker Kafka) |
