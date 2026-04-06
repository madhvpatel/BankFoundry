import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from sqlalchemy import text
from .calibration import estimate_recovery_rate, estimate_recoverable_value
from .constants import FAILED_STATUS_SQL
from .money import get_amount_scale, scale_inr

FAILED_GMV_SQL = f"""
SELECT SUM(amount_rupees) AS failed_gmv
FROM transaction_features
WHERE merchant_id = :mid
  AND p_date >= :start_date
  AND p_date < :end_date
  AND status IN {FAILED_STATUS_SQL}
"""


@dataclass
class ExperimentResult:
    experiment_name: str
    merchant_id: str
    generated_at: str
    window_days: int
    windows_evaluated: int
    mean_absolute_percentage_error: float | None
    mean_absolute_error_rupees: float | None
    details: list[dict]


def _failed_gmv(engine, mid: str, start_date, end_date) -> float:
    params = {"mid": mid, "start_date": start_date, "end_date": end_date}
    with engine.connect() as conn:
        row = conn.execute(text(FAILED_GMV_SQL), params).mappings().first() or {}
    return scale_inr(row.get("failed_gmv"), get_amount_scale(engine))


def run_recovery_backtest(
    engine,
    mid: str,
    end_date: dt.date,
    window_days: int = 30,
    n_windows: int = 3,
) -> ExperimentResult:
    details = []
    abs_pct_errors = []
    abs_errors = []

    for i in range(n_windows):
        test_end = end_date - dt.timedelta(days=i * window_days)
        test_start = test_end - dt.timedelta(days=window_days)
        train_end = test_start
        train_start = train_end - dt.timedelta(days=window_days)

        train_cal = estimate_recovery_rate(engine, mid, train_start, train_end)
        test_cal = estimate_recovery_rate(engine, mid, test_start, test_end)
        failed_gmv_test = _failed_gmv(engine, mid, test_start, test_end)

        predicted = estimate_recoverable_value(failed_gmv_test, train_cal)
        realized = estimate_recoverable_value(failed_gmv_test, test_cal)

        abs_error = abs(predicted - realized)
        abs_errors.append(abs_error)
        if realized > 0:
            abs_pct_errors.append(abs_error / realized)

        details.append({
            "train_start": str(train_start),
            "train_end": str(train_end),
            "test_start": str(test_start),
            "test_end": str(test_end),
            "failed_gmv_test": round(failed_gmv_test, 2),
            "predicted_recoverable": round(predicted, 2),
            "realized_recoverable_proxy": round(realized, 2),
            "train_rate": train_cal["rate"],
            "test_rate": test_cal["rate"],
            "train_retry_events": train_cal["retry_events"],
            "test_retry_events": test_cal["retry_events"],
        })

    mape = (sum(abs_pct_errors) / len(abs_pct_errors)) if abs_pct_errors else None
    mae = (sum(abs_errors) / len(abs_errors)) if abs_errors else None

    return ExperimentResult(
        experiment_name="recovery_rate_backtest",
        merchant_id=mid,
        generated_at=dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        window_days=window_days,
        windows_evaluated=n_windows,
        mean_absolute_percentage_error=round(mape, 4) if mape is not None else None,
        mean_absolute_error_rupees=round(mae, 2) if mae is not None else None,
        details=details,
    )


def append_experiment_log(result: ExperimentResult, output_path: str | None = None) -> str:
    if output_path:
        out = Path(output_path)
    else:
        root = Path(__file__).resolve().parents[2]
        out = root / "docs" / "run_time_docs" / "recovery_backtest_runs.jsonl"

    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "experiment_name": result.experiment_name,
        "merchant_id": result.merchant_id,
        "generated_at": result.generated_at,
        "window_days": result.window_days,
        "windows_evaluated": result.windows_evaluated,
        "mean_absolute_percentage_error": result.mean_absolute_percentage_error,
        "mean_absolute_error_rupees": result.mean_absolute_error_rupees,
        "details": result.details,
    }
    with out.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=True) + "\n")
    return str(out)
