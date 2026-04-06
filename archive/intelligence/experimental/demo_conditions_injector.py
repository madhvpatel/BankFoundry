from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
from dataclasses import dataclass
from typing import Any

from config import Config
from app.intelligence.demo_activity_generator import IST, PsqlClient


logger = logging.getLogger("demo_conditions_injector")
UTC = dt.timezone.utc


@dataclass(frozen=True)
class InjectorConfig:
    database_url: str
    merchant_id: str
    primary_terminal_id: str
    anomaly_terminal_id: str
    run_tag: str


@dataclass(frozen=True)
class TransactionPair:
    fact_row: dict[str, Any]
    feature_row: dict[str, Any]


@dataclass(frozen=True)
class ScenarioPlan:
    transactions: list[TransactionPair]
    refunds: list[dict[str, Any]]
    chargebacks: list[dict[str, Any]]
    settlements: list[dict[str, Any]]
    kyc_documents: list[dict[str, Any]]


def _utc(day: dt.date, hour: int, minute: int, second: int = 0, millis: int = 0) -> dt.datetime:
    local = dt.datetime.combine(day, dt.time(hour=hour, minute=minute, second=second, microsecond=millis * 1000), tzinfo=IST)
    return local.astimezone(UTC)


def _amount_bucket(amount_rupees: float) -> str:
    if amount_rupees <= 500:
        return "2. Medium (₹100-₹500)"
    if amount_rupees <= 2000:
        return "3. High (₹500-₹2000)"
    return "4. Very High (>₹2000)"


def _build_transaction_pair(
    *,
    merchant_id: str,
    terminal_id: str,
    source_txn_id: str,
    invoice_nr: str,
    p_date: dt.date,
    initiated_at: dt.datetime,
    completed_at: dt.datetime,
    payment_mode: str,
    status: str,
    amount_rupees: float,
    response_code: str | None,
    response_desc: str | None,
) -> TransactionPair:
    amount_paise = int(round(amount_rupees * 100))
    is_upi = payment_mode == "UPI"
    card_network = None if is_upi else "Visa Debit"
    card_type = None if is_upi else "VISA"
    sub_mode = "QR" if is_upi else "POS"

    fact_row = {
        "source_system": payment_mode,
        "source_txn_id": source_txn_id,
        "merchant_id": merchant_id,
        "terminal_id": terminal_id,
        "invoice_nr": invoice_nr,
        "initiated_at": initiated_at,
        "completed_at": completed_at,
        "p_date": p_date,
        "amount_paise": amount_paise,
        "currency": "INR",
        "status": status,
        "response_code": response_code,
        "response_desc": response_desc,
        "payment_mode": payment_mode,
        "sub_mode": sub_mode,
        "card_network": card_network,
        "card_type": card_type,
        "raw_card_autoid": None,
        "raw_upi_autoid": None,
        "expected_settlement_at": initiated_at + (dt.timedelta(hours=6) if is_upi else dt.timedelta(days=2)),
        "created_at": initiated_at,
    }
    feature_row = {
        "source_system": payment_mode,
        "source_txn_id": source_txn_id,
        "merchant_id": merchant_id,
        "terminal_id": terminal_id,
        "invoice_nr": invoice_nr,
        "raw_card_autoid": None,
        "raw_upi_autoid": None,
        "payment_mode": payment_mode,
        "sub_mode": sub_mode,
        "card_network": card_network,
        "card_type": card_type,
        "status": status,
        "response_code": response_code,
        "response_desc": response_desc,
        "currency": "INR",
        "amount_paise": amount_paise,
        "amount_rupees": amount_rupees,
        "amount_bucket": _amount_bucket(amount_rupees),
        "p_date": p_date,
        "initiated_at": initiated_at,
        "completed_at": completed_at,
        "expected_settlement_at": initiated_at + (dt.timedelta(hours=6) if is_upi else dt.timedelta(days=2)),
        "hour_of_day": initiated_at.astimezone(IST).hour,
        "day_of_week": p_date.weekday(),
        "is_weekend": p_date.weekday() >= 5,
        "is_night": initiated_at.astimezone(IST).hour < 6 or initiated_at.astimezone(IST).hour >= 22,
        "card_bin": None if is_upi else "436395",
        "mcc": "7523",
        "pos_entry_mode": None if is_upi else "051",
        "pos_type": None if is_upi else "",
        "device_type": "UPI_QR" if is_upi else "POS",
        "account_type": "SAVINGS" if is_upi else "Default",
        "is_pin_entered": False if is_upi else True,
        "is_moto_txn": False,
        "upi_app_name": "PhonePe" if is_upi else None,
        "upi_channel_code": "UPI" if is_upi else None,
        "upi_txn_type": "PAY" if is_upi else None,
        "upi_mcc_code": "7523" if is_upi else None,
        "payer_vpa_domain": "ybl" if is_upi else None,
        "payee_vpa_domain": "icici" if is_upi else None,
        "payer_ifsc": "HDFC0001234" if is_upi else None,
        "payee_ifsc": None,
        "payer_bank_code": "HDFC" if is_upi else None,
        "payee_bank_code": "ICIC" if is_upi else None,
        "payer_account_type": "SAVINGS" if is_upi else None,
        "time_since_prev_terminal_secs": None,
        "time_since_prev_merchant_secs": None,
        "terminal_txn_count_1h": None,
        "merchant_txn_count_1h": None,
        "terminal_success_rate_1h": None,
        "merchant_success_rate_1h": None,
        "created_at": initiated_at,
    }
    return TransactionPair(fact_row=fact_row, feature_row=feature_row)


def plan_alert_scenario(
    *,
    merchant_id: str,
    primary_terminal_id: str,
    anomaly_terminal_id: str,
    anchor_date: dt.date,
    run_tag: str,
) -> ScenarioPlan:
    transactions: list[TransactionPair] = []
    refunds: list[dict[str, Any]] = []
    chargebacks: list[dict[str, Any]] = []
    settlements: list[dict[str, Any]] = []
    kyc_documents: list[dict[str, Any]] = []
    seq = 0

    def add_tx(
        *,
        day: dt.date,
        hour: int,
        minute: int,
        delay_ms: int,
        terminal_id: str,
        payment_mode: str,
        status: str,
        amount_rupees: float,
        response_code: str | None,
        response_desc: str | None,
    ) -> None:
        nonlocal seq
        seq += 1
        initiated = _utc(day, hour, minute)
        completed = initiated + dt.timedelta(milliseconds=delay_ms)
        transactions.append(
            _build_transaction_pair(
                merchant_id=merchant_id,
                terminal_id=terminal_id,
                source_txn_id=f"{run_tag}-tx-{seq:04d}",
                invoice_nr=f"{seq:06d}",
                p_date=day,
                initiated_at=initiated,
                completed_at=completed,
                payment_mode=payment_mode,
                status=status,
                amount_rupees=amount_rupees,
                response_code=response_code,
                response_desc=response_desc,
            )
        )

    # Previous 7 days: clean, high-success baseline with fast UPI callbacks.
    for offset in range(7, 0, -1):
        day = anchor_date - dt.timedelta(days=offset)
        for i in range(10):
            add_tx(
                day=day,
                hour=10,
                minute=i,
                delay_ms=120,
                terminal_id=primary_terminal_id,
                payment_mode="UPI",
                status="SUCCESS",
                amount_rupees=500.0,
                response_code="00",
                response_desc="Transaction completed successfully",
            )
        for i in range(10):
            add_tx(
                day=day,
                hour=14,
                minute=i,
                delay_ms=60,
                terminal_id=anomaly_terminal_id,
                payment_mode="CARD",
                status="SUCCESS",
                amount_rupees=650.0,
                response_code="00",
                response_desc="Approved or completed successfully",
            )

    # Today: failing UPI on anomaly terminal to trigger terminal anomaly + callback delay + SR drop.
    for i in range(20):
        failed = i < 16
        add_tx(
            day=anchor_date,
            hour=9,
            minute=i,
            delay_ms=2500 if i == 0 else 1800,
            terminal_id=anomaly_terminal_id,
            payment_mode="UPI",
            status="FAILED" if failed else "SUCCESS",
            amount_rupees=100000.0 if i == 0 else (900.0 if failed else 700.0),
            response_code="91" if failed else "00",
            response_desc="Issuer or switch inoperative" if failed else "Transaction completed successfully",
        )

    # Today: healthy traffic on primary terminal.
    for i in range(20):
        add_tx(
            day=anchor_date,
            hour=11,
            minute=i,
            delay_ms=150,
            terminal_id=primary_terminal_id,
            payment_mode="UPI",
            status="SUCCESS",
            amount_rupees=520.0,
            response_code="00",
            response_desc="Transaction completed successfully",
        )
    for i in range(20):
        add_tx(
            day=anchor_date,
            hour=15,
            minute=i,
            delay_ms=80,
            terminal_id=primary_terminal_id,
            payment_mode="CARD",
            status="SUCCESS",
            amount_rupees=650.0,
            response_code="00",
            response_desc="Approved or completed successfully",
        )

    # Refund spike in the last 24h.
    for idx, amount in enumerate((320.0, 280.0, 450.0), start=1):
        refunds.append(
            {
                "source_system": "UPI",
                "source_txn_id": f"{run_tag}-refund-{idx:02d}",
                "mid": merchant_id,
                "tid": primary_terminal_id,
                "original_rrn": f"{run_tag}-rrn-r{idx:02d}",
                "refund_amount": amount,
                "refund_type": "FULL" if idx == 1 else "PARTIAL",
                "refund_reason": "Customer cancellation",
                "refund_status": "PROCESSED",
                "refund_date": _utc(anchor_date, 18, idx),
                "credit_date": _utc(anchor_date, 18, idx + 10),
                "refund_rrn": f"{run_tag}-refundrrn-{idx:02d}",
                "refund_auth_code": None,
                "mdr_reversed": True,
                "chargeback_id": None,
                "settlement_id": None,
                "p_date": anchor_date,
                "created_at": _utc(anchor_date, 18, idx),
            }
        )

    # Chargeback due within 48h.
    chargebacks.append(
        {
            "source_system": "UPI",
            "source_txn_id": f"{run_tag}-cb-01",
            "mid": merchant_id,
            "tid": anomaly_terminal_id,
            "original_rrn": f"{run_tag}-cbrrn-01",
            "original_auth_code": None,
            "chargeback_amount": 5000.0,
            "chargeback_currency": "INR",
            "chargeback_reason_code": "U028",
            "chargeback_reason_desc": "Customer dispute - service not received",
            "card_network": "NPCI_UPI",
            "chargeback_type": "FIRST_CHARGEBACK",
            "chargeback_stage": "OPEN",
            "filed_date": anchor_date,
            "response_due_date": anchor_date + dt.timedelta(days=1),
            "merchant_response_date": None,
            "merchant_response_status": "PENDING",
            "resolution_date": None,
            "resolution_outcome": None,
            "debit_credit_flag": "DEBIT",
            "representment_doc_ref": None,
            "arbitration_fee": 0.0,
            "settlement_id": None,
            "p_date": anchor_date,
            "created_at": _utc(anchor_date, 17, 0),
            "updated_at": _utc(anchor_date, 17, 0),
        }
    )

    # Settlement delay / hold older than 2 days.
    settlements.append(
        {
            "mid": merchant_id,
            "tid": primary_terminal_id,
            "settlement_date": anchor_date - dt.timedelta(days=3),
            "settlement_cycle": "T+1",
            "batch_number": f"{run_tag}-held-01",
            "gross_amount": 25000.0,
            "mdr_deducted": 100.0,
            "gst_on_mdr": 18.0,
            "tds_deducted": 0.0,
            "chargeback_deductions": 0.0,
            "reserve_held": 0.0,
            "adjustment_amount": 0.0,
            "net_settlement_amount": 24882.0,
            "bank_account_id": 1,
            "settlement_utr": f"{run_tag}-held-utr",
            "settlement_status": "HELD",
            "hold_reason": "Risk review",
            "txn_count": 12,
            "refund_count": 0,
            "payment_mode": "UPI",
            "p_date": anchor_date - dt.timedelta(days=3),
            "created_at": _utc(anchor_date - dt.timedelta(days=3), 8, 0),
        }
    )

    # KYC expiry / overdue.
    kyc_documents.extend(
        [
            {
                "mid": merchant_id,
                "document_type": "BUSINESS_LICENSE",
                "document_reference": f"{run_tag}-kyc-expiring",
                "document_number": None,
                "verified_flag": True,
                "verified_by": "demo-injector",
                "verified_date": anchor_date - dt.timedelta(days=90),
                "expiry_date": anchor_date + dt.timedelta(days=7),
                "kyc_status": "VERIFIED",
                "rejection_reason": None,
                "created_at": _utc(anchor_date, 8, 30),
            },
            {
                "mid": merchant_id,
                "document_type": "GST_CERTIFICATE",
                "document_reference": f"{run_tag}-kyc-overdue",
                "document_number": None,
                "verified_flag": True,
                "verified_by": "demo-injector",
                "verified_date": anchor_date - dt.timedelta(days=180),
                "expiry_date": anchor_date - dt.timedelta(days=1),
                "kyc_status": "VERIFIED",
                "rejection_reason": None,
                "created_at": _utc(anchor_date, 8, 35),
            },
        ]
    )

    return ScenarioPlan(
        transactions=transactions,
        refunds=refunds,
        chargebacks=chargebacks,
        settlements=settlements,
        kyc_documents=kyc_documents,
    )


def _to_float(value: str | None) -> float:
    return float(value) if value not in (None, "") else 0.0


def _to_int(value: str | None) -> int:
    return int(float(value)) if value not in (None, "") else 0


def _today_metrics(client: PsqlClient, merchant_id: str) -> dict[str, Any]:
    base = client._query_csv(
        f"""
        SELECT MAX(p_date) AS max_date
        FROM transaction_features
        WHERE merchant_id = {client._literal(merchant_id)}
        """
    )[0]
    max_date = dt.date.fromisoformat(base["max_date"])
    today_start = max_date
    today_end = max_date + dt.timedelta(days=1)
    prev7_start = max_date - dt.timedelta(days=7)
    prev7_end = max_date
    start_30 = today_end - dt.timedelta(days=30)
    due_end = max_date + dt.timedelta(days=2)
    delayed_cutoff = max_date - dt.timedelta(days=2)

    metrics: dict[str, Any] = {"max_date": max_date.isoformat()}

    row = client._query_csv(
        f"""
        SELECT
          COUNT(*) AS attempts_24h,
          SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_24h,
          SUM(CASE WHEN status IN ('FAILURE','FAILED') THEN 1 ELSE 0 END) AS fail_24h
        FROM transaction_features
        WHERE merchant_id = {client._literal(merchant_id)}
          AND p_date >= {client._literal(today_start)}
          AND p_date < {client._literal(today_end)}
        """
    )[0]
    attempts_24h = _to_int(row["attempts_24h"])
    success_24h = _to_int(row["success_24h"])
    fail_24h = _to_int(row["fail_24h"])
    success_rate_24h = (100.0 * success_24h / attempts_24h) if attempts_24h else 0.0

    row = client._query_csv(
        f"""
        SELECT
          COUNT(*) AS attempts_prev7,
          SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_prev7
        FROM transaction_features
        WHERE merchant_id = {client._literal(merchant_id)}
          AND p_date >= {client._literal(prev7_start)}
          AND p_date < {client._literal(prev7_end)}
        """
    )[0]
    attempts_prev7 = _to_int(row["attempts_prev7"])
    success_prev7 = _to_int(row["success_prev7"])
    success_rate_7d_avg = (100.0 * success_prev7 / attempts_prev7) if attempts_prev7 else 0.0

    row = client._query_csv(
        f"""
        SELECT
          percentile_cont(0.95)
            WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (completed_at - initiated_at)) * 1000) AS p95_today
        FROM transaction_features
        WHERE merchant_id = {client._literal(merchant_id)}
          AND UPPER(COALESCE(payment_mode, '')) = 'UPI'
          AND initiated_at IS NOT NULL
          AND completed_at IS NOT NULL
          AND p_date >= {client._literal(today_start)}
          AND p_date < {client._literal(today_end)}
        """
    )[0]
    callback_p95_today = _to_float(row["p95_today"])

    row = client._query_csv(
        f"""
        SELECT
          percentile_cont(0.95)
            WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (completed_at - initiated_at)) * 1000) AS p95_prev7
        FROM transaction_features
        WHERE merchant_id = {client._literal(merchant_id)}
          AND UPPER(COALESCE(payment_mode, '')) = 'UPI'
          AND initiated_at IS NOT NULL
          AND completed_at IS NOT NULL
          AND p_date >= {client._literal(prev7_start)}
          AND p_date < {client._literal(prev7_end)}
        """
    )[0]
    callback_p95_prev7 = _to_float(row["p95_prev7"])

    row = client._query_csv(
        f"""
        SELECT
          COUNT(*) AS refund_count_24h,
          COALESCE(SUM(refund_amount), 0) AS refund_gmv_24h
        FROM refunds
        WHERE mid = {client._literal(merchant_id)}
          AND p_date >= {client._literal(today_start)}
          AND p_date < {client._literal(today_end)}
        """
    )[0]
    refund_count_24h = _to_int(row["refund_count_24h"])

    row = client._query_csv(
        f"""
        SELECT COUNT(*) AS refund_count_prev7
        FROM refunds
        WHERE mid = {client._literal(merchant_id)}
          AND p_date >= {client._literal(prev7_start)}
          AND p_date < {client._literal(prev7_end)}
        """
    )[0]
    refund_count_prev7 = _to_int(row["refund_count_prev7"])

    chargeback_row = client._query_csv(
        f"""
        SELECT
          COUNT(*) AS chargeback_due_48h_count
        FROM chargebacks
        WHERE mid = {client._literal(merchant_id)}
          AND response_due_date IS NOT NULL
          AND response_due_date >= {client._literal(today_start)}
          AND response_due_date <= {client._literal(due_end)}
          AND (
            resolution_outcome IS NULL
            OR TRIM(resolution_outcome) = ''
            OR UPPER(TRIM(resolution_outcome)) IN ('OPEN', 'PENDING', 'IN_PROGRESS')
          )
        """
    )[0]

    settlement_row = client._query_csv(
        f"""
        SELECT
          COUNT(*) AS settlement_delayed_count
        FROM settlements
        WHERE mid = {client._literal(merchant_id)}
          AND UPPER(COALESCE(settlement_status, '')) IN ('PENDING', 'HELD', 'ON_HOLD', 'HOLD')
          AND settlement_date <= {client._literal(delayed_cutoff)}
        """
    )[0]

    high_value_row = client._query_csv(
        f"""
        WITH success_p95 AS (
          SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY amount_rupees) AS threshold
          FROM transaction_features
          WHERE merchant_id = {client._literal(merchant_id)}
            AND status = 'SUCCESS'
            AND p_date >= {client._literal(start_30)}
            AND p_date < {client._literal(today_end)}
        )
        SELECT
          COALESCE((SELECT threshold FROM success_p95), 0) AS high_value_ticket_threshold,
          COUNT(*) AS high_value_failed_count
        FROM transaction_features
        WHERE merchant_id = {client._literal(merchant_id)}
          AND status IN ('FAILURE','FAILED')
          AND p_date >= {client._literal(start_30)}
          AND p_date < {client._literal(today_end)}
          AND amount_rupees >= COALESCE((SELECT threshold FROM success_p95), 0)
        """
    )[0]

    terminal_row = client._query_csv(
        f"""
        WITH merchant_stats AS (
          SELECT
            COUNT(*) AS attempts_total,
            SUM(CASE WHEN status IN ('FAILURE','FAILED') THEN 1 ELSE 0 END) AS fail_total
          FROM transaction_features
          WHERE merchant_id = {client._literal(merchant_id)}
            AND p_date >= {client._literal(start_30)}
            AND p_date < {client._literal(today_end)}
        ),
        top_terminal AS (
          SELECT
            terminal_id,
            COUNT(*) AS attempts,
            SUM(CASE WHEN status IN ('FAILURE','FAILED') THEN 1 ELSE 0 END) AS fail_txns,
            ROUND(100.0 * SUM(CASE WHEN status IN ('FAILURE','FAILED') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS fail_rate_pct
          FROM transaction_features
          WHERE merchant_id = {client._literal(merchant_id)}
            AND terminal_id IS NOT NULL
            AND TRIM(terminal_id) <> ''
            AND p_date >= {client._literal(start_30)}
            AND p_date < {client._literal(today_end)}
          GROUP BY terminal_id
          HAVING COUNT(*) >= 20
          ORDER BY fail_rate_pct DESC, attempts DESC
          LIMIT 1
        )
        SELECT
          top_terminal.terminal_id AS top_terminal_id,
          top_terminal.attempts AS top_terminal_attempts,
          COALESCE(top_terminal.fail_rate_pct, 0) AS top_terminal_fail_rate_pct,
          ROUND(100.0 * merchant_stats.fail_total / NULLIF(merchant_stats.attempts_total, 0), 2) AS merchant_fail_rate_pct
        FROM merchant_stats, top_terminal
        """
    )[0]

    kyc_row = client._query_csv(
        f"""
        SELECT
          SUM(CASE WHEN expiry_date >= {client._literal(today_start)} AND expiry_date <= {client._literal(max_date + dt.timedelta(days=14))} THEN 1 ELSE 0 END) AS kyc_expiring_14d_count,
          SUM(CASE WHEN expiry_date < {client._literal(today_start)} THEN 1 ELSE 0 END) AS kyc_overdue_count
        FROM merchant_kyc_documents
        WHERE mid = {client._literal(merchant_id)}
          AND expiry_date IS NOT NULL
          AND UPPER(COALESCE(kyc_status, 'PENDING')) <> 'REJECTED'
        """
    )[0]

    merchant_fail_rate_pct = _to_float(terminal_row.get("merchant_fail_rate_pct"))
    top_terminal_fail_rate_pct = _to_float(terminal_row.get("top_terminal_fail_rate_pct"))
    terminal_fail_ratio = top_terminal_fail_rate_pct / merchant_fail_rate_pct if merchant_fail_rate_pct > 0 else 0.0
    callback_delay_ratio = callback_p95_today / callback_p95_prev7 if callback_p95_prev7 > 0 else 0.0
    refund_rate_24h = (100.0 * refund_count_24h / attempts_24h) if attempts_24h else 0.0
    refund_rate_7d_avg = (100.0 * refund_count_prev7 / attempts_prev7) if attempts_prev7 else 0.0
    success_rate_drop_pp = max(0.0, success_rate_7d_avg - success_rate_24h)

    metrics.update(
        {
            "attempts_24h": attempts_24h,
            "success_rate_24h": round(success_rate_24h, 2),
            "success_rate_7d_avg": round(success_rate_7d_avg, 2),
            "success_rate_drop_pp": round(success_rate_drop_pp, 2),
            "refund_count_24h": refund_count_24h,
            "refund_rate_24h": round(refund_rate_24h, 2),
            "refund_rate_7d_avg": round(refund_rate_7d_avg, 2),
            "chargeback_due_48h_count": _to_int(chargeback_row.get("chargeback_due_48h_count")),
            "settlement_delayed_count": _to_int(settlement_row.get("settlement_delayed_count")),
            "high_value_ticket_threshold": round(_to_float(high_value_row.get("high_value_ticket_threshold")), 2),
            "high_value_failed_count": _to_int(high_value_row.get("high_value_failed_count")),
            "top_terminal_id": terminal_row.get("top_terminal_id"),
            "top_terminal_attempts": _to_int(terminal_row.get("top_terminal_attempts")),
            "top_terminal_fail_rate_pct": round(top_terminal_fail_rate_pct, 2),
            "merchant_fail_rate_pct": round(merchant_fail_rate_pct, 2),
            "terminal_fail_ratio": round(terminal_fail_ratio, 2),
            "callback_delay_p95_ms_today": round(callback_p95_today, 2),
            "callback_delay_p95_ms_7d_avg": round(callback_p95_prev7, 2),
            "callback_delay_ratio": round(callback_delay_ratio, 2),
            "kyc_expiring_14d_count": _to_int(kyc_row.get("kyc_expiring_14d_count")),
            "kyc_overdue_count": _to_int(kyc_row.get("kyc_overdue_count")),
        }
    )
    metrics["conditions_met"] = {
        "chargeback_deadline": metrics["chargeback_due_48h_count"] >= 1,
        "high_value_failed_txns": metrics["high_value_failed_count"] >= 1,
        "kyc_expiry": metrics["kyc_expiring_14d_count"] >= 1 or metrics["kyc_overdue_count"] >= 1,
        "refund_rate_spike": metrics["refund_count_24h"] >= 2 and metrics["refund_rate_24h"] >= metrics["refund_rate_7d_avg"] * 1.5,
        "settlement_delay": metrics["settlement_delayed_count"] >= 1,
        "success_rate_drop": metrics["success_rate_drop_pp"] >= 1.5 and metrics["attempts_24h"] >= 50,
        "terminal_anomaly": metrics["top_terminal_attempts"] >= 20 and metrics["terminal_fail_ratio"] >= 3.0,
        "upi_callback_delay_spike": metrics["callback_delay_ratio"] >= 2.0 and metrics["callback_delay_p95_ms_today"] >= 500.0,
    }
    return metrics


def _ensure_terminal_sql(client: PsqlClient, merchant_id: str, terminal_id: str, created_at: dt.datetime) -> str:
    return f"""
        INSERT INTO terminals (
            tid, mid, terminal_serial_no, terminal_make, terminal_model, terminal_type,
            terminal_status, connectivity_type, app_version, deployment_date,
            last_txn_date, vpa, created_at, updated_at
        )
        SELECT
            {client._literal(terminal_id)},
            {client._literal(merchant_id)},
            {client._literal(f'SERIAL-{terminal_id[-6:]}')},
            'PAX',
            'A910S',
            'COUNTERTOP',
            'ACTIVE',
            'GPRS',
            'ICICI_A910S_v1.0.0.18',
            {client._literal(created_at.date())},
            {client._literal(created_at.date())},
            {client._literal(f'ibkPOS.{terminal_id}@icici')},
            {client._literal(created_at)},
            {client._literal(created_at)}
        WHERE NOT EXISTS (
            SELECT 1
            FROM terminals
            WHERE tid = {client._literal(terminal_id)}
        );
    """


def execute_injection(client: PsqlClient, config: InjectorConfig, plan: ScenarioPlan, anchor_date: dt.date) -> dict[str, Any]:
    fact_ids = client.next_transaction_fact_ids(len(plan.transactions))
    created_at = _utc(anchor_date, 8, 0)
    statements = ["BEGIN;"]
    statements.append(_ensure_terminal_sql(client, config.merchant_id, config.anomaly_terminal_id, created_at))

    for fact_id, pair in zip(fact_ids, plan.transactions):
        statements.append(client.insert_sql("transaction_fact", {"transaction_fact_id": fact_id, **pair.fact_row}))
        statements.append(client.insert_sql("transaction_features", {"transaction_fact_id": fact_id, **pair.feature_row}))

    for row in plan.refunds:
        statements.append(client.insert_sql("refunds", row))
    for row in plan.chargebacks:
        statements.append(client.insert_sql("chargebacks", row))
    for row in plan.settlements:
        statements.append(client.insert_sql("settlements", row))
    for row in plan.kyc_documents:
        statements.append(client.insert_sql("merchant_kyc_documents", row))

    for terminal_id in {config.primary_terminal_id, config.anomaly_terminal_id}:
        statements.append(
            f"""
            UPDATE terminals
            SET last_txn_date = {client._literal(anchor_date)},
                updated_at = {client._literal(created_at)}
            WHERE tid = {client._literal(terminal_id)};
            """
        )
    statements.append("COMMIT;")

    client._run(sql="", input_sql="\n".join(statements))
    metrics = _today_metrics(client, config.merchant_id)
    return {
        "run_tag": config.run_tag,
        "anchor_date": anchor_date.isoformat(),
        "inserted": {
            "transactions": len(plan.transactions),
            "refunds": len(plan.refunds),
            "chargebacks": len(plan.chargebacks),
            "settlements": len(plan.settlements),
            "kyc_documents": len(plan.kyc_documents),
        },
        "metrics": metrics,
    }


def parse_args() -> InjectorConfig:
    parser = argparse.ArgumentParser(
        description="Inject a deterministic dataset that activates all configured insight-card conditions.",
    )
    parser.add_argument("--database-url", default=Config.DATABASE_URL, help="Postgres connection URL.")
    parser.add_argument("--merchant-id", default="100000000121215", help="Merchant MID to target.")
    parser.add_argument("--primary-terminal-id", default="EP070270", help="Healthy terminal ID.")
    parser.add_argument("--anomaly-terminal-id", default="EP070271", help="Failing terminal ID used for terminal anomaly.")
    parser.add_argument(
        "--run-tag",
        default=dt.datetime.now(IST).strftime("alertseed-%Y%m%d-%H%M%S"),
        help="Unique tag prefix for injected records.",
    )
    args = parser.parse_args()
    return InjectorConfig(
        database_url=str(args.database_url),
        merchant_id=str(args.merchant_id),
        primary_terminal_id=str(args.primary_terminal_id),
        anomaly_terminal_id=str(args.anomaly_terminal_id),
        run_tag=str(args.run_tag),
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = parse_args()
    client = PsqlClient(config.database_url)

    # Validate merchant / primary terminal existence up front.
    client.load_context(config.merchant_id, config.primary_terminal_id)

    anchor_date = dt.datetime.now(IST).date()
    plan = plan_alert_scenario(
        merchant_id=config.merchant_id,
        primary_terminal_id=config.primary_terminal_id,
        anomaly_terminal_id=config.anomaly_terminal_id,
        anchor_date=anchor_date,
        run_tag=config.run_tag,
    )
    summary = execute_injection(client, config, plan, anchor_date)
    print(json.dumps(summary, ensure_ascii=True, indent=2, default=str))


if __name__ == "__main__":
    main()
