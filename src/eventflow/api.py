from __future__ import annotations

from fastapi import BackgroundTasks, FastAPI

from .sample_pipeline import build_runner


app = FastAPI(title="EventFlow MLOps", version="0.1.0")
runner = build_runner()


def run_job(job_id: str) -> None:
    runner.run(job_id)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/pipelines/{pipeline_name}/jobs")
def submit_job(pipeline_name: str, payload: dict, background_tasks: BackgroundTasks) -> dict[str, str]:
    job_id = runner.submit(pipeline_name, payload)
    background_tasks.add_task(run_job, job_id)
    return {"job_id": job_id, "status": "queued"}


@app.get("/jobs/{job_id}")
def get_job(job_id: str) -> dict:
    job = runner.store.get_job(job_id)
    if job is None:
        return {"status": "not_found"}
    return job

