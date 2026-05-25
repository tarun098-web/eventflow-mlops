from __future__ import annotations

from dataclasses import dataclass
import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4


Task = Callable[[dict[str, Any]], dict[str, Any]]


@dataclass(frozen=True)
class Step:
    name: str
    task: str
    retries: int = 1


@dataclass(frozen=True)
class Pipeline:
    name: str
    steps: list[Step]


class WorkflowStore:
    def __init__(self, db_path: str | Path = "eventflow.db") -> None:
        self.db_path = str(db_path)
        self._init()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    pipeline TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    result_json TEXT,
                    error TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS steps (
                    job_id TEXT NOT NULL,
                    step_name TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT,
                    created_at REAL NOT NULL
                )
                """
            )

    def create_job(self, pipeline: str, payload: dict[str, Any]) -> str:
        job_id = str(uuid4())
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO jobs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (job_id, pipeline, "queued", json.dumps(payload), None, None, now, now),
            )
        return job_id

    def update_job(self, job_id: str, status: str, result: dict[str, Any] | None = None, error: str | None = None) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE jobs SET status = ?, result_json = ?, error = ?, updated_at = ? WHERE id = ?",
                (status, json.dumps(result) if result is not None else None, error, time.time(), job_id),
            )

    def record_step(self, job_id: str, step_name: str, attempt: int, status: str, error: str | None = None) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO steps VALUES (?, ?, ?, ?, ?, ?)",
                (job_id, step_name, attempt, status, error, time.time()),
            )

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id, pipeline, status, payload_json, result_json, error, created_at, updated_at FROM jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
            steps = conn.execute(
                "SELECT step_name, attempt, status, error FROM steps WHERE job_id = ? ORDER BY created_at",
                (job_id,),
            ).fetchall()
        if row is None:
            return None
        return {
            "id": row[0],
            "pipeline": row[1],
            "status": row[2],
            "payload": json.loads(row[3]),
            "result": json.loads(row[4]) if row[4] else None,
            "error": row[5],
            "created_at": row[6],
            "updated_at": row[7],
            "steps": [
                {"step_name": step[0], "attempt": step[1], "status": step[2], "error": step[3]}
                for step in steps
            ],
        }


class PipelineRunner:
    def __init__(self, store: WorkflowStore | None = None) -> None:
        self.store = store or WorkflowStore()
        self.tasks: dict[str, Task] = {}
        self.pipelines: dict[str, Pipeline] = {}

    def register_task(self, name: str, task: Task) -> None:
        self.tasks[name] = task

    def register_pipeline(self, pipeline: Pipeline) -> None:
        self.pipelines[pipeline.name] = pipeline

    def submit(self, pipeline_name: str, payload: dict[str, Any]) -> str:
        if pipeline_name not in self.pipelines:
            raise KeyError(f"Unknown pipeline: {pipeline_name}")
        return self.store.create_job(pipeline_name, payload)

    def run(self, job_id: str) -> dict[str, Any]:
        job = self.store.get_job(job_id)
        if job is None:
            raise KeyError(f"Unknown job: {job_id}")
        pipeline = self.pipelines[job["pipeline"]]
        state = dict(job["payload"])
        self.store.update_job(job_id, "running")

        try:
            for step in pipeline.steps:
                state = self._run_step(job_id, step, state)
            self.store.update_job(job_id, "completed", result=state)
            return state
        except Exception as exc:
            self.store.update_job(job_id, "failed", error=str(exc))
            raise

    def _run_step(self, job_id: str, step: Step, state: dict[str, Any]) -> dict[str, Any]:
        task = self.tasks[step.task]
        last_error: Exception | None = None
        for attempt in range(1, step.retries + 2):
            try:
                output = task(dict(state))
                state.update(output)
                self.store.record_step(job_id, step.name, attempt, "completed")
                return state
            except Exception as exc:
                last_error = exc
                self.store.record_step(job_id, step.name, attempt, "failed", str(exc))
        raise RuntimeError(f"Step '{step.name}' failed after retries: {last_error}")

