from __future__ import annotations

from typing import Any

from .core import Pipeline, PipelineRunner, Step


def validate_payload(state: dict[str, Any]) -> dict[str, Any]:
    required = {"customer_id", "income", "debt"}
    missing = required - set(state)
    if missing:
        raise ValueError(f"Missing fields: {sorted(missing)}")
    return {"validated": True}


def score_risk(state: dict[str, Any]) -> dict[str, Any]:
    income = float(state["income"])
    debt = float(state["debt"])
    debt_ratio = debt / max(income, 1)
    risk_score = min(1.0, max(0.0, 0.25 + debt_ratio))
    return {"debt_ratio": round(debt_ratio, 4), "risk_score": round(risk_score, 4)}


def create_decision(state: dict[str, Any]) -> dict[str, Any]:
    decision = "manual_review" if state["risk_score"] >= 0.55 else "approve"
    return {"decision": decision}


def build_runner() -> PipelineRunner:
    runner = PipelineRunner()
    runner.register_task("validate_payload", validate_payload)
    runner.register_task("score_risk", score_risk)
    runner.register_task("create_decision", create_decision)
    runner.register_pipeline(
        Pipeline(
            name="risk-score",
            steps=[
                Step("validate", "validate_payload", retries=0),
                Step("score", "score_risk", retries=1),
                Step("decision", "create_decision", retries=0),
            ],
        )
    )
    return runner

