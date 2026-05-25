# EventFlow MLOps

A local event-driven ML workflow orchestrator inspired by cloud/serverless architecture, but fully offline.

This project demonstrates production-style orchestration, retries, job state, metrics, and API design without requiring AWS or paid services.

## Output Proof

- Open [demo/index.html](demo/index.html) for a visual sample workflow run dashboard.
- Review [sample_outputs/risk_score_job.json](sample_outputs/risk_score_job.json) for a concrete job-status API output.
- See [docs/demo.md](docs/demo.md) for what the demo proves.

## What This Demonstrates

- Event-driven pipeline design.
- Async job execution patterns.
- Retryable task orchestration.
- SQLite-backed run tracking.
- Local ML inference workflow.
- API design for job submission and status checks.

## Features

- Define pipelines as ordered steps.
- Register Python functions as pipeline tasks.
- Run jobs with retry handling.
- Persist job status, step attempts, outputs, and errors in SQLite.
- Expose FastAPI endpoints for submitting and inspecting jobs.
- Include a sample risk scoring workflow.

## Local Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e ".[dev]"
pytest
```

Run the API:

```bash
uvicorn eventflow.api:app --reload
```

Submit a job:

```bash
curl -X POST http://127.0.0.1:8000/pipelines/risk-score/jobs -H "Content-Type: application/json" -d "{\"customer_id\":\"C-100\",\"income\":72000,\"debt\":12000}"
```

## Limitations

- This is a local orchestrator for learning and portfolio demonstration.
- It is not a replacement for Airflow, Prefect, Dagster, or AWS Step Functions.
- Workers run in-process in v1.
