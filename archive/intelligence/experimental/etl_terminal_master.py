from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any, Iterable, Mapping


POS_SUCCESS_CODES = {"00", "0", "000"}
UPI_SUCCESS_VALUES = {"SUCCESS", "APPROVED", "COMPLETED"}
UPI_FAILED_VALUES = {"FAILED", "FAILURE", "DECLINED", "REVERSED", "TIMEOUT"}


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _to_float(value: Any) -> float | None:
    s = _clean(value)
    if s is None:
        return None
    try:
        return float(s.replace(",", ""))
    except Exception:
        return None


def _parse_datetime(value: Any) -> dt.datetime | None:
    s = _clean(value)
    if s is None:
        return None

    candidates = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%d-%m-%Y",
        "%Y-%m-%d",
        "%d/%m/%Y",
    )
    for fmt in candidates:
        try:
            parsed = dt.datetime.strptime(s, fmt)
            if "H" not in fmt:
                return dt.datetime.combine(parsed.date(), dt.time.min)
            return parsed
        except Exception:
            continue
    return None


def _parse_date(value: Any) -> dt.date | None:
    d = _parse_datetime(value)
    return d.date() if d else None


def _iso_date(value: dt.date | None) -> str | None:
    return value.isoformat() if value else None


def _iso_dt(value: dt.datetime | None) -> str | None:
    return value.isoformat(sep=" ") if value else None


def _normalize_pos_status(code: Any, desc: Any) -> str:
    c = (_clean(code) or "").upper()
    d = (_clean(desc) or "").upper()
    if c in POS_SUCCESS_CODES or "APPROVED" in d or "SUCCESS" in d:
        return "SUCCESS"
    return "FAILED"


def _normalize_upi_status(txn_status: Any) -> str:
    v = (_clean(txn_status) or "").upper()
    if v in UPI_SUCCESS_VALUES:
        return "SUCCESS"
    if v in UPI_FAILED_VALUES:
        return "FAILED"
    if not v:
        return "UNKNOWN"
    return "UNKNOWN"


def _derive_time_fields(initiated_at: dt.datetime | None, p_date: dt.date | None) -> tuple[int | None, int | None]:
    if initiated_at:
        return initiated_at.hour, initiated_at.weekday()
    if p_date:
        return 0, p_date.weekday()
    return None, None


def map_postransaction_row(row: Mapping[str, Any]) -> dict[str, Any]:
    initiated_at = _parse_datetime(row.get("request_datetime") or row.get("inserted_on"))
    completed_at = _parse_datetime(row.get("rsp_datetime"))
    p_date = _parse_date(row.get("tran_date")) or (initiated_at.date() if initiated_at else None)
    hour_of_day, day_of_week = _derive_time_fields(initiated_at, p_date)

    return {
        "merchant_id": _clean(row.get("mid")),
        "terminal_id": _clean(row.get("tid")),
        "source_system": "POS",
        "source_txn_id": _clean(row.get("tran_id") or row.get("rrn")),
        "invoice_nr": _clean(row.get("invoice_nr")),
        "payment_mode": "CARD",
        "status": _normalize_pos_status(row.get("rsp_code"), row.get("rsp_desc")),
        "response_code": _clean(row.get("rsp_code")),
        "response_desc": _clean(row.get("rsp_desc")),
        "amount_rupees": _to_float(row.get("amount") or row.get("sale_amt")),
        "p_date": _iso_date(p_date),
        "initiated_at": _iso_dt(initiated_at),
        "completed_at": _iso_dt(completed_at),
        "hour_of_day": hour_of_day,
        "day_of_week": day_of_week,
        "card_type": _clean(row.get("card_type")),
        "card_network": _clean(row.get("network_type")),
        "pos_entry_mode": _clean(row.get("pos_entry_mode")),
    }


def map_upitransaction_row(row: Mapping[str, Any]) -> dict[str, Any]:
    initiated_at = _parse_datetime(row.get("txninitdate"))
    completed_at = _parse_datetime(row.get("txncompletiondate"))
    p_date = _parse_date(row.get("p_date")) or _parse_date(row.get("txninitdate")) or _parse_date(row.get("txncompletiondate"))
    hour_of_day, day_of_week = _derive_time_fields(initiated_at, p_date)

    return {
        "merchant_id": _clean(row.get("mid")),
        "terminal_id": _clean(row.get("tid")),
        "source_system": "UPI",
        "source_txn_id": _clean(row.get("upitranlogid") or row.get("rrn")),
        "invoice_nr": _clean(row.get("invoice_nr")),
        "payment_mode": "UPI",
        "status": _normalize_upi_status(row.get("txnstatus")),
        "response_code": _clean(row.get("responsecode") or row.get("payer_respcode") or row.get("payee_respcode")),
        "response_desc": _clean(row.get("txnstatus")),
        "amount_rupees": _to_float(row.get("amount") or row.get("sale_amt")),
        "p_date": _iso_date(p_date),
        "initiated_at": _iso_dt(initiated_at),
        "completed_at": _iso_dt(completed_at),
        "hour_of_day": hour_of_day,
        "day_of_week": day_of_week,
        "mcc": _clean(row.get("payee_mcccode")),
    }


def normalize_records(
    pos_rows: Iterable[Mapping[str, Any]],
    upi_rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in pos_rows:
        out.append(map_postransaction_row(r))
    for r in upi_rows:
        out.append(map_upitransaction_row(r))
    return out


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=True, default=str) + "\n")


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_mode: dict[str, int] = {}
    by_status: dict[str, int] = {}
    for r in rows:
        mode = str(r.get("payment_mode") or "UNKNOWN")
        status = str(r.get("status") or "UNKNOWN")
        by_mode[mode] = by_mode.get(mode, 0) + 1
        by_status[status] = by_status.get(status, 0) + 1
    return {
        "rows": len(rows),
        "by_payment_mode": by_mode,
        "by_status": by_status,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Normalize POS/UPI exports into transaction_features-compatible JSONL.",
    )
    parser.add_argument("--pos-csv", type=Path, required=True, help="CSV export for postransactions.")
    parser.add_argument("--upi-csv", type=Path, required=True, help="CSV export for upitransactions.")
    parser.add_argument("--out-jsonl", type=Path, required=True, help="Output normalized JSONL path.")
    parser.add_argument("--limit", type=int, default=0, help="Optional row cap per source (0 means all).")
    args = parser.parse_args()

    pos = _read_csv(args.pos_csv)
    upi = _read_csv(args.upi_csv)
    if args.limit and args.limit > 0:
        pos = pos[: args.limit]
        upi = upi[: args.limit]

    rows = normalize_records(pos_rows=pos, upi_rows=upi)
    _write_jsonl(args.out_jsonl, rows)

    print(json.dumps({"output": str(args.out_jsonl), "summary": _summarize(rows)}, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
