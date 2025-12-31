You are a senior Staff-level Data Engineer and Software Architect.
Build a production-grade, end-to-end Financial Forecasting Data Platform suitable for a mid-level Data Engineer resume and interview portfolio.

🎯 GOAL

Create a real-world data engineering system that ingests raw business events, enforces data quality, models analytics-ready financial data, runs automated forecasts, and serves insights.
This must not be a toy project. Design it as if it will run in production with real stakeholders.

🧱 REQUIRED ARCHITECTURE
1. Data Ingestion

Build Python services (FastAPI) that ingest:

subscription events

payment events

usage events

operational cost events

Support:

event-based ingestion via REST

scheduled batch ingestion

Store all raw events immutably in a data lake (S3 or GCS).

Ensure idempotency and replayability.

2. Data Validation & Quality Controls

Enforce schemas for all event types.

Implement:

null checks

range checks

duplicate detection

late-arriving data handling

Route invalid records to quarantine tables.

Fail pipelines on critical data quality issues.

Produce data quality metrics and logs.

3. Data Modeling Layer

Transform raw data into analytics-ready tables using SQL.

Use a star schema with:

Fact tables:

revenue_daily

subscriptions_snapshot

costs_daily

usage_daily

Dimension tables:

customer

plan

region

time

Support schema evolution and backfills.

Implement dbt-style transformations (dbt optional but preferred).

4. Forecasting Engine

Build automated forecasting jobs that:

forecast daily revenue

forecast active subscriptions

compute forecast vs actual variance

Use:

baseline statistical models (ARIMA or Prophet)

Store:

forecast outputs

confidence intervals

model metadata for reproducibility

5. Orchestration & Reliability

Orchestrate pipelines using Airflow, Prefect, or Dagster.

Support:

retries

backfills

dependency-aware execution

Implement structured logging and metrics.

Include alerting for data quality failures.

6. Analytics & Serving Layer

Expose analytics via:

SQL-backed APIs OR

BI dashboards (Metabase or Superset)

Required metrics:

MRR

churn

forecast vs actual

revenue by region

7. Infrastructure & CI/CD

Dockerize all services.

Provide:

CI pipeline (tests, linting)

CD pipeline

Infrastructure as code using Terraform or CDK.

Deploy on AWS or GCP.

📁 REQUIRED REPO STRUCTURE
financial-forecasting-platform/
├── ingestion/
├── validation/
├── transformations/
├── forecasting/
├── orchestration/
├── analytics/
├── infra/
├── tests/
└── README.md

📄 README REQUIREMENTS

Clear system overview

Architecture diagram (ASCII or Mermaid)

Local setup instructions

Data model explanations

Tradeoffs and design decisions

How this mirrors real finance engineering systems

🧪 QUALITY BAR

Production-quality code

Typed Python

Tests for critical logic

No shortcuts

No “hello world” implementations

🎓 OUTPUT EXPECTATION

Generate:

Complete code

Config files

SQL models

Infrastructure definitions

Documentation

“Do not simplify this project. Build it as if it will be reviewed by senior data engineers at Spotify.”