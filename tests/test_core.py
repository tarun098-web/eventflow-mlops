import pytest

from eventflow.core import Pipeline, PipelineRunner, Step, WorkflowStore
from eventflow.sample_pipeline import build_runner


def test_pipeline_completes(tmp_path):
    runner = build_runner()
    runner.store = WorkflowStore(tmp_path / "flow.db")
    job_id = runner.submit("risk-score", {"customer_id": "C-1", "income": 72000, "debt": 12000})
    result = runner.run(job_id)
    assert result["decision"] == "approve"
    assert runner.store.get_job(job_id)["status"] == "completed"


def test_unknown_pipeline_fails(tmp_path):
    runner = PipelineRunner(WorkflowStore(tmp_path / "flow.db"))
    with pytest.raises(KeyError):
        runner.submit("missing", {})


def test_missing_payload_field_records_failure(tmp_path):
    runner = build_runner()
    runner.store = WorkflowStore(tmp_path / "flow.db")
    job_id = runner.submit("risk-score", {"customer_id": "C-1", "income": 72000})
    with pytest.raises(ValueError):
        runner.run(job_id)
    assert runner.store.get_job(job_id)["status"] == "failed"


def test_retries_eventually_succeed(tmp_path):
    attempts = {"count": 0}

    def flaky(state):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("temporary")
        return {"ok": True}

    runner = PipelineRunner(WorkflowStore(tmp_path / "flow.db"))
    runner.register_task("flaky", flaky)
    runner.register_pipeline(Pipeline("demo", [Step("flaky-step", "flaky", retries=1)]))
    job_id = runner.submit("demo", {})
    result = runner.run(job_id)
    assert result["ok"] is True
    assert attempts["count"] == 2


def test_retry_exhaustion_fails(tmp_path):
    def broken(state):
        raise RuntimeError("always broken")

    runner = PipelineRunner(WorkflowStore(tmp_path / "flow.db"))
    runner.register_task("broken", broken)
    runner.register_pipeline(Pipeline("demo", [Step("broken-step", "broken", retries=1)]))
    job_id = runner.submit("demo", {})
    with pytest.raises(RuntimeError):
        runner.run(job_id)
    steps = runner.store.get_job(job_id)["steps"]
    assert len(steps) == 2


def test_store_returns_step_history(tmp_path):
    runner = build_runner()
    runner.store = WorkflowStore(tmp_path / "flow.db")
    job_id = runner.submit("risk-score", {"customer_id": "C-2", "income": 30000, "debt": 25000})
    runner.run(job_id)
    job = runner.store.get_job(job_id)
    assert len(job["steps"]) == 3
    assert job["result"]["decision"] == "manual_review"

