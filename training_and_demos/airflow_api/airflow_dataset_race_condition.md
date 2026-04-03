# Airflow Dataset Event Race Condition

## Problem

When multiple dataset events are triggered in rapid succession (e.g., via the Airflow REST API), the downstream DAG that depends on those datasets may not get triggered for every event. The DAG then never runs until another dataset event arrives in the future.

This commonly occurs when:

- Calling `POST /api/v1/datasets/events` multiple times back-to-back from a script
- AWS Lambda functions fire in parallel (e.g., 4 files uploaded to S3 trigger 4 simultaneous Lambda invocations, each posting a dataset event)
- Multiple upstream DAG tasks complete near-simultaneously

## Root Cause

The Airflow scheduler processes dataset events in a loop controlled by `scheduler_heartbeat_sec` (default: 5 seconds). A **TOCTOU (time-of-check-to-time-of-use) race condition** exists in the scheduler's `DatasetDagRunQueue` (DDRQ) processing:

1. **Event A** arrives, is inserted into `dataset_event`, DDRQ is updated
2. Scheduler picks up the DDRQ entry, creates a DAG run, **clears the DDRQ**
3. **Event B** arrives milliseconds later, is inserted into `dataset_event`, DDRQ is updated
4. The scheduler already processed that slot — Event B is recorded but **never triggers a DAG run** until a future event arrives

This is a known issue tracked in several GitHub issues including:

- [apache/airflow#56750](https://github.com/apache/airflow/issues/56750) — Grouped Issue: Asset Scheduling behavior changes based on DAG performance settings (consolidated tracker)
- [apache/airflow#54659](https://github.com/apache/airflow/issues/54659) — Asset-triggered DAGs miss events from concurrently completing dynamic mapped tasks

## Status

| Airflow Version | Status |
|-----------------|--------|
| 2.4 - 2.8 | Bug present |
| 2.9 - 2.10 | Partially improved |
| 3.0 | Datasets were reworked as "Assets" with improved atomicity and event processing guarantees, but there are still open issues |

## Recommendation

- Develop a process that does not trigger so many simultaneous events
