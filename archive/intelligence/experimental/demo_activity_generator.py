from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import io
import json
import logging
import random
import signal
import string
import subprocess
import time
import uuid
from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config import Config


logger = logging.getLogger("demo_activity_generator")

IST = ZoneInfo("Asia/Kolkata")
UTC = dt.timezone.utc

UPI_APPS = (
    {"name": "PhonePe", "domain": "ybl"},
    {"name": "Google Pay", "domain": "okhdfcbank"},
    {"name": "Paytm", "domain": "paytm"},
    {"name": "BHIM", "domain": "upi"},
    {"name": "Amazon Pay", "domain": "apl"},
)

BANKS = (
    {"code": "HDFC", "ifsc": "HDFC0001234", "domain": "okhdfcbank"},
    {"code": "ICIC", "ifsc": "ICIC0004321", "domain": "okicici"},
    {"code": "SBIN", "ifsc": "SBIN0009876", "domain": "upi"},
    {"code": "UTIB", "ifsc": "UTIB0002468", "domain": "ibl"},
    {"code": "KKBK", "ifsc": "KKBK0001357", "domain": "axl"},
)

UPI_FAILURES = (
    ("U16", "UPI request timed out"),
    ("91", "Issuer or switch inoperative"),
    ("ZM", "Invalid VPA"),
    ("U30", "Customer declined the collect request"),
    ("51", "Insufficient funds"),
)

CARD_FAILURES = (
    ("05", "Do not honor"),
    ("51", "Insufficient funds"),
    ("55", "Incorrect PIN"),
    ("91", "Issuer or switch inoperative"),
)

CARD_PRODUCTS = (
    {
        "network_label": "Visa Debit",
        "card_type_code": "VISA",
        "rate_network": "VISA",
        "rate_card_type": "DEBIT",
        "bin": "436395",
        "entry_mode": "051",
    },
    {
        "network_label": "Mastercard Debit",
        "card_type_code": "MASTERCARD",
        "rate_network": "MASTERCARD",
        "rate_card_type": "DEBIT",
        "bin": "517698",
        "entry_mode": "051",
    },
    {
        "network_label": "RuPay Debit",
        "card_type_code": "RUPAY",
        "rate_network": "RUPAY",
        "rate_card_type": "DEBIT",
        "bin": "608088",
        "entry_mode": "051",
    },
    {
        "network_label": "Visa Credit",
        "card_type_code": "VISA",
        "rate_network": "VISA",
        "rate_card_type": "CREDIT",
        "bin": "453215",
        "entry_mode": "071",
    },
    {
        "network_label": "Mastercard Credit",
        "card_type_code": "MASTERCARD",
        "rate_network": "MASTERCARD",
        "rate_card_type": "CREDIT",
        "bin": "545901",
        "entry_mode": "071",
    },
)

PAYER_NAMES = (
    "Aarav Sharma",
    "Neha Gupta",
    "Rahul Verma",
    "Priya Singh",
    "Vikram Nair",
    "Ananya Mehta",
    "Rohan Kapoor",
    "Karan Malhotra",
    "Sneha Das",
    "Maya Iyer",
)

REFUND_REASONS = (
    "Duplicate payment",
    "Vehicle exited quickly",
    "Wrong amount charged",
)

CHARGEBACK_REASONS = (
    ("12.1", "Late presentment or delayed service"),
    ("13.6", "Customer disputes transaction"),
    ("U028", "Customer dispute - service not received"),
)


@dataclass(frozen=True)
class BusinessPreset:
    profile_id: str
    mcc_code: str
    trade_name_prefix: str
    legal_name_prefix: str
    merchant_type: str
    city: str
    state: str
    pincode: str
    nature_of_business: str
    annual_turnover: float
    expected_monthly_volume: float
    expected_avg_ticket_size: float
    terminal_make: str
    terminal_model: str
    terminal_type: str
    connectivity_type: str
    traffic_weight: int
    default_terminal_count: int
    latitude: float
    longitude: float
    low_network_probability: float
    quick_battery_probability: float
    geo_drift_probability: float


BUSINESS_PRESETS: dict[str, BusinessPreset] = {
    "airport_parking": BusinessPreset(
        profile_id="airport_parking",
        mcc_code="7523",
        trade_name_prefix="Acqui Parking",
        legal_name_prefix="Acqui Parking Services",
        merchant_type="PVT_LTD",
        city="New Delhi",
        state="DELHI",
        pincode="110037",
        nature_of_business="Airport parking and valet services",
        annual_turnover=120000000.0,
        expected_monthly_volume=10000000.0,
        expected_avg_ticket_size=550.0,
        terminal_make="PAX",
        terminal_model="A910S",
        terminal_type="COUNTERTOP",
        connectivity_type="GPRS",
        traffic_weight=2,
        default_terminal_count=1,
        latitude=28.5562,
        longitude=77.0999,
        low_network_probability=0.06,
        quick_battery_probability=0.04,
        geo_drift_probability=0.01,
    ),
    "grocery_store": BusinessPreset(
        profile_id="grocery_store",
        mcc_code="5411",
        trade_name_prefix="Acqui Grocery",
        legal_name_prefix="Acqui Grocery Retail",
        merchant_type="PVT_LTD",
        city="Bengaluru",
        state="KARNATAKA",
        pincode="560001",
        nature_of_business="Grocery store and supermarket retail",
        annual_turnover=90000000.0,
        expected_monthly_volume=7500000.0,
        expected_avg_ticket_size=850.0,
        terminal_make="PAX",
        terminal_model="A920",
        terminal_type="COUNTERTOP",
        connectivity_type="4G",
        traffic_weight=3,
        default_terminal_count=2,
        latitude=12.9716,
        longitude=77.5946,
        low_network_probability=0.03,
        quick_battery_probability=0.03,
        geo_drift_probability=0.005,
    ),
    "petrol_pump": BusinessPreset(
        profile_id="petrol_pump",
        mcc_code="5541",
        trade_name_prefix="Acqui Fuel",
        legal_name_prefix="Acqui Fuel Services",
        merchant_type="PVT_LTD",
        city="Mumbai",
        state="MAHARASHTRA",
        pincode="400001",
        nature_of_business="Petrol pump and forecourt retail",
        annual_turnover=240000000.0,
        expected_monthly_volume=20000000.0,
        expected_avg_ticket_size=2200.0,
        terminal_make="Ingenico",
        terminal_model="DX8000",
        terminal_type="FORECOURT",
        connectivity_type="4G",
        traffic_weight=4,
        default_terminal_count=4,
        latitude=19.076,
        longitude=72.8777,
        low_network_probability=0.04,
        quick_battery_probability=0.02,
        geo_drift_probability=0.005,
    ),
    "cloud_kitchen": BusinessPreset(
        profile_id="cloud_kitchen",
        mcc_code="5814",
        trade_name_prefix="Acqui Kitchen",
        legal_name_prefix="Acqui Kitchen Foods",
        merchant_type="PVT_LTD",
        city="Hyderabad",
        state="TELANGANA",
        pincode="500081",
        nature_of_business="Cloud kitchen and food delivery operations",
        annual_turnover=48000000.0,
        expected_monthly_volume=4000000.0,
        expected_avg_ticket_size=480.0,
        terminal_make="Sunmi",
        terminal_model="V2 Pro",
        terminal_type="MOBILE",
        connectivity_type="WIFI",
        traffic_weight=2,
        default_terminal_count=2,
        latitude=17.385,
        longitude=78.4867,
        low_network_probability=0.02,
        quick_battery_probability=0.05,
        geo_drift_probability=0.02,
    ),
    "big_merchant": BusinessPreset(
        profile_id="big_merchant",
        mcc_code="5311",
        trade_name_prefix="Acqui Hypermart",
        legal_name_prefix="Acqui Hypermart Retail",
        merchant_type="PUBLIC_LTD",
        city="Mumbai",
        state="MAHARASHTRA",
        pincode="400051",
        nature_of_business="Large-format retail and multi-counter checkout",
        annual_turnover=960000000.0,
        expected_monthly_volume=80000000.0,
        expected_avg_ticket_size=3500.0,
        terminal_make="Verifone",
        terminal_model="V400c",
        terminal_type="COUNTERTOP",
        connectivity_type="LAN",
        traffic_weight=6,
        default_terminal_count=6,
        latitude=19.0596,
        longitude=72.8295,
        low_network_probability=0.015,
        quick_battery_probability=0.01,
        geo_drift_probability=0.002,
    ),
}

PRESET_BY_MCC = {preset.mcc_code: preset for preset in BUSINESS_PRESETS.values()}


def _preset_for_profile(profile_id: str) -> BusinessPreset:
    if profile_id not in BUSINESS_PRESETS:
        raise ValueError(f"Unsupported business profile: {profile_id}")
    return BUSINESS_PRESETS[profile_id]


def _preset_for_mcc(mcc_code: str | None) -> BusinessPreset:
    return PRESET_BY_MCC.get(str(mcc_code or ""), BUSINESS_PRESETS["airport_parking"])


@dataclass(frozen=True)
class PortfolioSpec:
    profile_id: str
    merchant_count: int


@dataclass(frozen=True)
class MerchantContext:
    mid: str
    tid: str
    merchant_trade_name: str
    merchant_legal_name: str
    business_city: str
    business_state: str
    mcc_code: str
    expected_avg_ticket_size: float
    max_transaction_limit: float
    vpa: str
    terminal_make: str
    terminal_model: str
    app_version: str
    primary_bank_account_id: int | None
    terminal_serial_no: str = ""
    location_latitude: float | None = None
    location_longitude: float | None = None
    connectivity_type: str = ""
    terminal_type: str = ""
    profile_id: str = "custom"
    traffic_weight: int = 1


@dataclass(frozen=True)
class GeneratorConfig:
    database_url: str
    merchant_id: str | None = None
    terminal_id: str | None = None
    interval_seconds: float = 5.0
    batch_min: int = 2
    batch_max: int = 5
    upi_share: float = 0.8
    upi_success_rate: float = 0.97
    card_success_rate: float = 0.94
    settlement_every_batches: int = 4
    refund_probability: float = 0.0
    chargeback_probability: float = 0.0
    terminal_health_every_batches: int = 3
    fraud_alert_probability: float = 0.05
    fee_ledger_enabled: bool = True
    business_profile: str | None = None
    portfolio: str | None = None
    merchant_count: int = 1
    terminal_count: int = 0
    provision_only: bool = False
    max_batches: int = 0
    once: bool = False
    dry_run: bool = False
    seed: int | None = None


@dataclass(frozen=True)
class InsertSpec:
    table: str
    row: dict[str, Any]


@dataclass
class GeneratedTransaction:
    payment_mode: str
    status: str
    amount_rupees: float
    amount_paise: int
    rrn: str
    source_txn_id: str
    mdr_rate_pct: float
    raw_inserts: list[InsertSpec]
    fact_row: dict[str, Any]
    feature_row: dict[str, Any]
    card_rate_network: str | None = None
    card_rate_type: str | None = None


@dataclass
class GeneratedBatch:
    transactions: list[GeneratedTransaction]
    settlements: list[dict[str, Any]]
    refunds: list[dict[str, Any]]
    chargebacks: list[dict[str, Any]]
    terminal_health_snapshots: list[dict[str, Any]]
    fraud_alerts: list[dict[str, Any]]
    fee_ledger_rows: list[dict[str, Any]]


def _utc(dt_value: dt.datetime) -> dt.datetime:
    return dt_value.astimezone(UTC)


def _fmt_text_ts(dt_value: dt.datetime) -> str:
    return dt_value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _digits(value: int, width: int) -> str:
    return str(value % (10 ** width)).zfill(width)


def _random_hex(rng: random.Random, length: int) -> str:
    chars = string.hexdigits.lower()[:16]
    return "".join(rng.choice(chars) for _ in range(length))


def _masked_card_number(bin_value: str, rng: random.Random) -> str:
    suffix = "".join(rng.choice(string.digits) for _ in range(4))
    return f"{bin_value[:6]}******{suffix}"


def _amount_bucket(amount_rupees: float) -> str:
    if amount_rupees <= 500:
        return "2. Medium (₹100-₹500)"
    if amount_rupees <= 2000:
        return "3. High (₹500-₹2000)"
    return "4. Very High (>₹2000)"


def _sample_amount_rupees(rng: random.Random, context: MerchantContext, payment_mode: str) -> float:
    if payment_mode == "UPI":
        roll = rng.random()
        if roll < 0.60:
            low, high = 180.0, 900.0
        elif roll < 0.85:
            low, high = 900.0, 2500.0
        elif roll < 0.97:
            low, high = 2500.0, 6000.0
        else:
            low, high = 6000.0, min(context.max_transaction_limit, 12000.0)
    else:
        roll = rng.random()
        if roll < 0.70:
            low, high = 150.0, 700.0
        elif roll < 0.95:
            low, high = 700.0, 2000.0
        else:
            low, high = 2000.0, min(context.max_transaction_limit, 5000.0)

    amount = rng.uniform(low, high)
    if amount < context.expected_avg_ticket_size * 0.6:
        amount = (amount + context.expected_avg_ticket_size) / 2.0
    amount = min(amount, context.max_transaction_limit)
    amount = max(amount, 120.0)
    return round(amount / 10.0) * 10.0


def _context_from_row(row: dict[str, Any] | Any) -> MerchantContext:
    preset = _preset_for_mcc(row["mcc_code"])
    lat = row.get("location_latitude")
    lon = row.get("location_longitude")
    return MerchantContext(
        mid=str(row["mid"]),
        tid=str(row["tid"]),
        merchant_trade_name=str(row["merchant_trade_name"]),
        merchant_legal_name=str(row["merchant_legal_name"]),
        business_city=str(row["business_city"]),
        business_state=str(row["business_state"]),
        mcc_code=str(row["mcc_code"]),
        expected_avg_ticket_size=float(row["expected_avg_ticket_size"] or preset.expected_avg_ticket_size),
        max_transaction_limit=float(row["max_transaction_limit"] or 100000.0),
        vpa=str(row["vpa"]),
        terminal_make=str(row["terminal_make"]),
        terminal_model=str(row["terminal_model"]),
        app_version=str(row["app_version"]),
        primary_bank_account_id=int(row["primary_bank_account_id"]) if row["primary_bank_account_id"] is not None and str(row["primary_bank_account_id"]) != "" else None,
        terminal_serial_no=str(row.get("terminal_serial_no") or ""),
        location_latitude=float(lat) if lat not in (None, "") else None,
        location_longitude=float(lon) if lon not in (None, "") else None,
        connectivity_type=str(row.get("connectivity_type") or ""),
        terminal_type=str(row.get("terminal_type") or ""),
        profile_id=preset.profile_id,
        traffic_weight=preset.traffic_weight,
    )


def _load_context(engine: Engine, merchant_id: str | None, terminal_id: str | None) -> MerchantContext:
    sql = text(
        """
        SELECT
          m.mid,
          t.tid,
          COALESCE(m.merchant_trade_name, m.merchant_legal_name, m.mid) AS merchant_trade_name,
          COALESCE(m.merchant_legal_name, m.merchant_trade_name, m.mid) AS merchant_legal_name,
          COALESCE(m.business_city, '') AS business_city,
          COALESCE(m.business_state, '') AS business_state,
          COALESCE(m.mcc_code, '7523') AS mcc_code,
          COALESCE(m.expected_avg_ticket_size, 550.0) AS expected_avg_ticket_size,
          COALESCE(c.max_transaction_limit, 100000.0) AS max_transaction_limit,
          COALESCE(t.vpa, 'merchant@icici') AS vpa,
          COALESCE(t.terminal_make, '') AS terminal_make,
          COALESCE(t.terminal_model, '') AS terminal_model,
          COALESCE(t.app_version, '') AS app_version,
          COALESCE(t.terminal_serial_no, '') AS terminal_serial_no,
          t.location_latitude,
          t.location_longitude,
          COALESCE(t.connectivity_type, '') AS connectivity_type,
          COALESCE(t.terminal_type, '') AS terminal_type,
          mba.account_id AS primary_bank_account_id
        FROM merchants m
        JOIN terminals t
          ON t.mid = m.mid
        LEFT JOIN mcc_codes c
          ON c.mcc_code = m.mcc_code
        LEFT JOIN merchant_bank_accounts mba
          ON mba.mid = m.mid
         AND COALESCE(mba.is_primary, false) = true
        WHERE (:mid IS NULL OR m.mid = :mid)
          AND (:tid IS NULL OR t.tid = :tid)
        ORDER BY m.mid, t.tid
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"mid": merchant_id, "tid": terminal_id}).mappings().first()
    if not row:
        raise RuntimeError("No merchant/terminal pair found in the database.")
    return _context_from_row(dict(row))


def _load_mdr_rates(engine: Engine, mid: str) -> dict[tuple[str, str, str], dict[str, Any]]:
    sql = text(
        """
        SELECT payment_mode, COALESCE(card_network, '') AS card_network,
               COALESCE(card_type, '') AS card_type,
               mdr_id,
               COALESCE(mdr_percentage, 0) AS mdr_percentage,
               COALESCE(gst_on_mdr_pct, 18) AS gst_on_mdr_pct,
               COALESCE(interchange_rate, 0) AS interchange_rate,
               COALESCE(acquirer_margin, 0) AS acquirer_margin,
               COALESCE(scheme_fee, 0) AS scheme_fee
        FROM mdr_rates
        WHERE mid = :mid
        """
    )
    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    with engine.connect() as conn:
        rows = conn.execute(sql, {"mid": mid}).mappings().all()
    for row in rows:
        key = (
            str(row["payment_mode"] or "").upper(),
            str(row["card_network"] or "").upper(),
            str(row["card_type"] or "").upper(),
        )
        out[key] = {
            "mdr_id": int(row["mdr_id"]) if row["mdr_id"] is not None else None,
            "mdr_percentage": float(row["mdr_percentage"] or 0.0),
            "gst_on_mdr_pct": float(row["gst_on_mdr_pct"] or 18.0),
            "interchange_rate": float(row["interchange_rate"] or 0.0),
            "acquirer_margin": float(row["acquirer_margin"] or 0.0),
            "scheme_fee": float(row["scheme_fee"] or 0.0),
        }
    return out


def _load_counters(engine: Engine) -> tuple[int, int, int, int]:
    sql = text(
        """
        SELECT
          GREATEST(
            COALESCE((SELECT MAX(autoid) FROM raw_upi_transactions), 0),
            COALESCE((SELECT MAX(autoid) FROM raw_upi_qr_records), 0),
            COALESCE((SELECT MAX(autoid) FROM raw_upi_mqtt_logs), 0),
            COALESCE((SELECT MAX(auto_id) FROM raw_upi_notifications), 0),
            COALESCE((SELECT MAX(id) FROM raw_upi_callback_logs), 0),
            COALESCE((SELECT MAX(autoid) FROM raw_card_transactions), 0),
            0
          ) AS max_raw_id,
          GREATEST(
            COALESCE((SELECT MAX(CAST(rrn AS bigint)) FROM raw_upi_transactions WHERE rrn ~ '^[0-9]+$'), 0),
            COALESCE((SELECT MAX(CAST(rrn AS bigint)) FROM raw_card_transactions WHERE rrn ~ '^[0-9]+$'), 0),
            0
          ) AS max_rrn,
          COALESCE(
            (SELECT MAX(CAST(invoice_nr AS bigint))
             FROM transaction_fact
             WHERE invoice_nr ~ '^[0-9]+$'),
            0
          ) AS max_invoice,
          GREATEST(
            COALESCE((SELECT MAX(CAST(stan AS bigint)) FROM raw_upi_transactions WHERE stan ~ '^[0-9]+$'), 0),
            COALESCE((SELECT MAX(CAST(stan AS bigint)) FROM raw_card_transactions WHERE stan ~ '^[0-9]+$'), 0),
            0
          ) AS max_stan
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql).mappings().one()
    return (
        int(row["max_raw_id"] or 0),
        int(row["max_rrn"] or 0),
        int(row["max_invoice"] or 0),
        int(row["max_stan"] or 0),
    )


def build_settlement_rows(
    successful_transactions: list[GeneratedTransaction],
    *,
    context: MerchantContext,
    when_local: dt.datetime,
    batch_number: int,
    gst_pct: float = 18.0,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    grouped: dict[str, list[GeneratedTransaction]] = {}
    for txn in successful_transactions:
        grouped.setdefault(txn.payment_mode, []).append(txn)

    when_utc = _utc(when_local)
    for payment_mode, txns in grouped.items():
        gross_amount = round(sum(txn.amount_rupees for txn in txns), 2)
        mdr_deducted = round(sum(txn.amount_rupees * txn.mdr_rate_pct / 100.0 for txn in txns), 2)
        gst_on_mdr = round(mdr_deducted * (gst_pct / 100.0), 2) if mdr_deducted else 0.0
        net_amount = round(gross_amount - mdr_deducted - gst_on_mdr, 2)

        rows.append(
            {
                "mid": context.mid,
                "tid": context.tid,
                "settlement_date": when_local.date(),
                "settlement_cycle": "T+0",
                "batch_number": f"LIVE-{when_local:%Y%m%d}-{batch_number:04d}-{payment_mode}",
                "gross_amount": gross_amount,
                "mdr_deducted": mdr_deducted,
                "gst_on_mdr": gst_on_mdr,
                "tds_deducted": 0.0,
                "chargeback_deductions": 0.0,
                "reserve_held": 0.0,
                "adjustment_amount": 0.0,
                "net_settlement_amount": net_amount,
                "bank_account_id": context.primary_bank_account_id,
                "settlement_utr": f"UTR{when_local:%Y%m%d%H%M%S}{payment_mode[:1]}{batch_number:03d}",
                "settlement_status": "PROCESSED",
                "hold_reason": None,
                "txn_count": len(txns),
                "refund_count": 0,
                "payment_mode": payment_mode,
                "p_date": when_local.date(),
                "created_at": when_utc,
            }
        )
    return rows


class DemoActivityGenerator:
    def __init__(self, engine: Engine, config: GeneratorConfig, contexts: list[MerchantContext] | None = None):
        self.engine = engine
        self.config = config
        self.rng = random.Random(config.seed)
        self.contexts = contexts or [_load_context(engine, config.merchant_id, config.terminal_id)]
        self.context = self.contexts[0]
        self.mdr_rates = _load_mdr_rates(engine, self.context.mid)
        self._raw_id, self._rrn_counter, self._invoice_counter, self._stan_counter = _load_counters(engine)
        self._continue = True
        self._batch_counter = 0

    def stop(self, *_args: object) -> None:
        self._continue = False

    def install_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    def _next_raw_id(self) -> int:
        self._raw_id += 1
        return self._raw_id

    def _next_rrn(self) -> str:
        self._rrn_counter += self.rng.randint(7, 29)
        return _digits(self._rrn_counter, 12)

    def _next_invoice(self) -> str:
        self._invoice_counter += 1
        return _digits(self._invoice_counter, 6)

    def _next_stan(self) -> str:
        self._stan_counter += 1
        return _digits(self._stan_counter, 6)

    def _select_context(self) -> MerchantContext:
        if len(self.contexts) == 1:
            return self.contexts[0]
        return self.rng.choices(self.contexts, weights=[max(1, ctx.traffic_weight) for ctx in self.contexts], k=1)[0]

    def _ensure_rate_table_for_context(self, context: MerchantContext) -> None:
        if self.context.mid != context.mid:
            self.context = context
            self.mdr_rates = _load_mdr_rates(self.engine, context.mid)

    def _resolve_rate_card(self, payment_mode: str, card_network: str = "", card_type: str = "") -> dict[str, Any]:
        key = (payment_mode.upper(), card_network.upper(), card_type.upper())
        fallback = self.mdr_rates.get((payment_mode.upper(), "", ""), {})
        value = self.mdr_rates.get(key, fallback)
        if isinstance(value, tuple):
            return {
                "mdr_id": None,
                "mdr_percentage": float(value[0] if len(value) > 0 else 0.0),
                "gst_on_mdr_pct": float(value[1] if len(value) > 1 else 18.0),
                "interchange_rate": 0.0,
                "acquirer_margin": 0.0,
                "scheme_fee": 0.0,
            }
        return {
            "mdr_id": value.get("mdr_id"),
            "mdr_percentage": float(value.get("mdr_percentage", 0.0) or 0.0),
            "gst_on_mdr_pct": float(value.get("gst_on_mdr_pct", 18.0) or 18.0),
            "interchange_rate": float(value.get("interchange_rate", 0.0) or 0.0),
            "acquirer_margin": float(value.get("acquirer_margin", 0.0) or 0.0),
            "scheme_fee": float(value.get("scheme_fee", 0.0) or 0.0),
        }

    def _resolve_mdr_rate(self, payment_mode: str, card_network: str = "", card_type: str = "") -> float:
        return self._resolve_rate_card(payment_mode, card_network, card_type)["mdr_percentage"]

    def _choose_status(self, payment_mode: str) -> tuple[str, str, str]:
        if payment_mode == "UPI":
            if self.rng.random() <= self.config.upi_success_rate:
                return "SUCCESS", "00", "Transaction completed successfully"
            code, desc = self.rng.choice(UPI_FAILURES)
            return "FAILED", code, desc
        if self.rng.random() <= self.config.card_success_rate:
            return "SUCCESS", "00", "Approved or completed successfully"
        code, desc = self.rng.choice(CARD_FAILURES)
        return "FAILED", code, desc

    def _build_upi_transaction(self, when_local: dt.datetime) -> GeneratedTransaction:
        when_utc = _utc(when_local)
        amount_rupees = _sample_amount_rupees(self.rng, self.context, "UPI")
        amount_paise = int(round(amount_rupees * 100))
        status, response_code, response_desc = self._choose_status("UPI")

        payer_name = self.rng.choice(PAYER_NAMES)
        payer_bank = self.rng.choice(BANKS)
        upi_app = self.rng.choice(UPI_APPS)

        raw_id = self._next_raw_id()
        notification_id = self._next_raw_id()
        callback_id = self._next_raw_id()
        qr_id = self._next_raw_id()
        mqtt_id = self._next_raw_id()

        invoice_nr = self._next_invoice()
        stan = self._next_stan()
        rrn = self._next_rrn()
        upi_tranlog_id = str(self._next_raw_id())
        profile_id = _digits(self._next_raw_id(), 9)
        refid = f"EPPSDQR{self.context.tid}{when_local:%d%m%y%H%M%S}{invoice_nr}"
        payer_mobile = _digits(self._next_raw_id(), 10)
        payer_va = f"{_digits(self._next_raw_id(), 10)}@{upi_app['domain']}"

        callback_payload = {
            "MessageType": "1200",
            "ProcCode": "UPI055",
            "NotificationId": notification_id,
            "TargetMobile": "8800223558",
            "TxnType": "PAY",
            "ProfileId": profile_id,
            "UpiTranlogId": upi_tranlog_id,
            "ExpireAfter": "0",
            "PayerAccount": _digits(self._next_raw_id(), 10),
            "PayerAccountType": "SAVINGS",
            "Payee": {
                "Name": f"{self.context.merchant_legal_name} RECEIVABLES ACCOUNT",
                "Mobile": "8800223558",
                "VA": self.context.vpa,
                "RespCode": "00",
                "MccCode": self.context.mcc_code,
                "MccType": "ENTITY",
                "AccountNo": "",
                "Ifsc": "",
                "RevRespCode": "",
                "VerifiedMerchant": "Y",
            },
            "Payer": {
                "Name": payer_name,
                "Mobile": payer_mobile,
                "VA": payer_va,
                "Ifsc": payer_bank["ifsc"],
                "RespCode": response_code,
                "RevRespCode": "",
            },
            "Amount": f"{amount_rupees:.2f}",
            "ChannelCode": "UPI",
            "TxnStatus": status,
            "TxnInitDate": when_local.strftime("%Y%m%d%H%M%S"),
            "TxnCompletionDate": when_local.strftime("%Y%m%d%H%M%S"),
            "Note": f"Parking payment via {upi_app['name']}",
            "DeviceId": self.context.tid,
            "OriginalTxnId": uuid.uuid4().hex,
            "SeqNo": "",
            "RefId": refid,
            "RefUrl": "http://www.npci.org.in/",
            "Rrn": rrn,
            "ResponseCode": response_code,
            "UMN": "",
        }
        callback_json = json.dumps(callback_payload, ensure_ascii=True, separators=(",", ":"))
        notification_body = json.dumps(
            {
                "transactionid": upi_tranlog_id,
                "mid": self.context.mid,
                "tid": self.context.tid,
                "rrn": rrn,
                "status": status,
            },
            ensure_ascii=True,
            separators=(",", ":"),
        )
        mqtt_message = json.dumps(
            {"refid": refid, "rrn": rrn, "status": status, "amount": f"{amount_rupees:.2f}"},
            ensure_ascii=True,
            separators=(",", ":"),
        )

        raw_rows = [
            InsertSpec(
                table="raw_upi_transactions",
                row={
                    "autoid": raw_id,
                    "mid": self.context.mid,
                    "tid": self.context.tid,
                    "invoice_nr": invoice_nr,
                    "amount": _digits(amount_paise, 12),
                    "rrn": rrn,
                    "txnstatus": status,
                    "stan": stan,
                    "batchnr": _digits(self._batch_counter, 6),
                    "messagetype": "1200",
                    "proccode": "UPI055",
                    "notificationid": str(notification_id),
                    "targetmobile": "8800223558",
                    "txntype": "PAY",
                    "profileid": profile_id,
                    "upitranlogid": upi_tranlog_id,
                    "expireafter": "0",
                    "payee_name": f"{self.context.merchant_legal_name} RECEIVABLES ACCOUNT",
                    "payee_mobile": "8800223558",
                    "payee_va": self.context.vpa,
                    "payee_respcode": "00",
                    "payee_mcccode": self.context.mcc_code,
                    "payee_mcctype": "ENTITY",
                    "payee_accountno": "",
                    "payee_ifsc": "",
                    "payee_revrespcode": "",
                    "payer_name": payer_name,
                    "payer_mobile": payer_mobile,
                    "payer_va": payer_va,
                    "payer_ifsc": payer_bank["ifsc"],
                    "payer_respcode": response_code,
                    "payer_revrespcode": "",
                    "payer_accountno": _digits(self._next_raw_id(), 10),
                    "channelcode": "UPI",
                    "txninitdate": _fmt_text_ts(when_local),
                    "txncompletiondate": _fmt_text_ts(when_local),
                    "note": f"Parking payment via {upi_app['name']}",
                    "deviceid": self.context.tid,
                    "originaltxnid": uuid.uuid4().hex,
                    "seqno": "",
                    "refid": refid,
                    "refurl": "http://www.npci.org.in/",
                    "responsecode": response_code,
                    "umn": "",
                    "inserted_on": _fmt_text_ts(when_local),
                    "created_date": _fmt_text_ts(when_local),
                    "txn_cancelled": "",
                    "callbackjson": callback_json,
                    "tran_type": "16",
                    "billnumber": invoice_nr,
                    "tip_amt": "000000000000",
                    "sale_amt": _digits(amount_paise, 12),
                    "checkstatusmode": "",
                    "p_date": when_local.date(),
                    "payeraccounttype": "SAVINGS",
                    "payeraccount": _digits(self._next_raw_id(), 10),
                    "appname": upi_app["name"],
                },
            ),
            InsertSpec(
                table="raw_upi_notifications",
                row={
                    "auto_id": notification_id,
                    "transactionid": upi_tranlog_id,
                    "mid": self.context.mid,
                    "tid": self.context.tid,
                    "notificationrequest": notification_body,
                    "status": status,
                    "created_date": _fmt_text_ts(when_local),
                    "iteration": "1",
                    "response": "{\"ack\":\"OK\"}",
                    "p_date": when_local.date(),
                },
            ),
            InsertSpec(
                table="raw_upi_callback_logs",
                row={
                    "id": callback_id,
                    "refid": refid,
                    "rrn": rrn,
                    "json_request": callback_json,
                    "inserted_on": _fmt_text_ts(when_local),
                    "txn_status": status,
                    "p_date": when_local.date(),
                },
            ),
            InsertSpec(
                table="raw_upi_qr_records",
                row={
                    "autoid": qr_id,
                    "mid": self.context.mid,
                    "tid": self.context.tid,
                    "amount": _digits(amount_paise, 12),
                    "sequenceno": stan,
                    "invoice_nr": invoice_nr,
                    "refid": refid,
                    "created_at": _fmt_text_ts(when_local),
                    "vpa": self.context.vpa,
                    "stan": stan,
                    "batchnr": _digits(self._batch_counter, 6),
                    "logrefid": refid,
                    "qr_expired": "N",
                    "tran_type": "PAY",
                    "billnumber": invoice_nr,
                    "tipamount": "000000000000",
                    "sale_amt": _digits(amount_paise, 12),
                    "checkstatusmode": "",
                    "p_date": when_local.date(),
                },
            ),
            InsertSpec(
                table="raw_upi_mqtt_logs",
                row={
                    "autoid": mqtt_id,
                    "message": mqtt_message,
                    "response": "{\"published\":true}",
                    "mqttconfig": "demo-live-feed",
                    "insertedon": _fmt_text_ts(when_local),
                },
            ),
        ]

        fact_row = {
            "source_system": "UPI",
            "source_txn_id": upi_tranlog_id,
            "merchant_id": self.context.mid,
            "terminal_id": self.context.tid,
            "invoice_nr": invoice_nr,
            "initiated_at": when_utc,
            "completed_at": when_utc,
            "p_date": when_local.date(),
            "amount_paise": amount_paise,
            "currency": "INR",
            "status": status,
            "response_code": response_code,
            "response_desc": response_desc,
            "payment_mode": "UPI",
            "sub_mode": "QR",
            "card_network": None,
            "card_type": None,
            "raw_card_autoid": None,
            "raw_upi_autoid": str(raw_id),
            "expected_settlement_at": when_utc + dt.timedelta(hours=6),
            "created_at": when_utc,
        }
        feature_row = {
            "source_system": "UPI",
            "source_txn_id": upi_tranlog_id,
            "merchant_id": self.context.mid,
            "terminal_id": self.context.tid,
            "invoice_nr": invoice_nr,
            "raw_card_autoid": None,
            "raw_upi_autoid": str(raw_id),
            "payment_mode": "UPI",
            "sub_mode": "QR",
            "card_network": None,
            "card_type": None,
            "status": status,
            "response_code": response_code,
            "response_desc": response_desc,
            "currency": "INR",
            "amount_paise": amount_paise,
            "amount_rupees": round(amount_paise / 100.0, 2),
            "amount_bucket": _amount_bucket(amount_rupees),
            "p_date": when_local.date(),
            "initiated_at": when_utc,
            "completed_at": when_utc,
            "expected_settlement_at": when_utc + dt.timedelta(hours=6),
            "hour_of_day": when_local.hour,
            "day_of_week": when_local.weekday(),
            "is_weekend": when_local.weekday() >= 5,
            "is_night": when_local.hour < 6 or when_local.hour >= 22,
            "card_bin": None,
            "mcc": self.context.mcc_code,
            "pos_entry_mode": None,
            "pos_type": None,
            "device_type": "UPI_QR",
            "account_type": "SAVINGS",
            "is_pin_entered": False,
            "is_moto_txn": False,
            "upi_app_name": upi_app["name"],
            "upi_channel_code": "UPI",
            "upi_txn_type": "PAY",
            "upi_mcc_code": self.context.mcc_code,
            "payer_vpa_domain": payer_va.split("@", 1)[-1],
            "payee_vpa_domain": self.context.vpa.split("@", 1)[-1],
            "payer_ifsc": payer_bank["ifsc"],
            "payee_ifsc": None,
            "payer_bank_code": payer_bank["code"],
            "payee_bank_code": "ICIC",
            "payer_account_type": "SAVINGS",
            "time_since_prev_terminal_secs": None,
            "time_since_prev_merchant_secs": None,
            "terminal_txn_count_1h": None,
            "merchant_txn_count_1h": None,
            "terminal_success_rate_1h": None,
            "merchant_success_rate_1h": None,
            "created_at": when_utc,
        }
        return GeneratedTransaction(
            payment_mode="UPI",
            status=status,
            amount_rupees=round(amount_paise / 100.0, 2),
            amount_paise=amount_paise,
            rrn=rrn,
            source_txn_id=upi_tranlog_id,
            mdr_rate_pct=self._resolve_mdr_rate("UPI"),
            raw_inserts=raw_rows,
            fact_row=fact_row,
            feature_row=feature_row,
            card_rate_network=None,
            card_rate_type=None,
        )

    def _build_card_transaction(self, when_local: dt.datetime) -> GeneratedTransaction:
        when_utc = _utc(when_local)
        when_naive = when_local.replace(tzinfo=None)
        amount_rupees = _sample_amount_rupees(self.rng, self.context, "CARD")
        amount_paise = int(round(amount_rupees * 100))
        status, response_code, response_desc = self._choose_status("CARD")
        product = self.rng.choices(CARD_PRODUCTS, weights=[24, 18, 18, 10, 8], k=1)[0]

        raw_id = self._next_raw_id()
        invoice_nr = self._next_invoice()
        rrn = self._next_rrn()
        stan = self._next_stan()
        auth_code = _digits(self._next_raw_id(), 6) if status == "SUCCESS" else None
        tran_id = str(uuid.uuid4())
        request_hash = hashlib.sha256(f"{tran_id}:{rrn}".encode("utf-8")).hexdigest().upper()

        raw_rows = [
            InsertSpec(
                table="raw_card_transactions",
                row={
                    "autoid": raw_id,
                    "tid": self.context.tid,
                    "mid": self.context.mid,
                    "crd_no": _masked_card_number(product["bin"], self.rng),
                    "amount": amount_paise,
                    "rrn": rrn,
                    "rsp_code": response_code,
                    "rsp_desc": response_desc,
                    "tran_date": when_naive,
                    "mti": "0200",
                    "tran_type": 1,
                    "proc_code": "000000",
                    "stan": stan,
                    "is_settled": False,
                    "tran_id": tran_id,
                    "auth_code": auth_code,
                    "batch_nr": _digits(self._batch_counter, 6),
                    "receip_tname": "",
                    "tip_amt": 0,
                    "ip_adrs": "192.168.250.10:3536",
                    "card_holder_name": "",
                    "etdata": _random_hex(self.rng, 80).upper(),
                    "ksn": _random_hex(self.rng, 20).upper(),
                    "encpan": _random_hex(self.rng, 32).upper(),
                    "icc_data": _random_hex(self.rng, 160).upper(),
                    "pos_entry_mode": product["entry_mode"],
                    "pos_con_code": "00",
                    "invoice_nr": invoice_nr,
                    "pin_flag": "",
                    "pos_type": "",
                    "addtional_data": "{\"ADF1\":\"\",\"ADF2\":null}",
                    "app_ver": self.context.app_version or "ICICI_A910S_v1.0.0.18",
                    "card_type": product["card_type_code"],
                    "network_type": product["network_label"],
                    "se_number": "",
                    "nii": "0110",
                    "aid": "A0000000041010",
                    "tvr": "0000008001",
                    "tsi": "0000",
                    "app_name": self.context.app_version or "ICICI_A910S_v1.0.0.18",
                    "npci_rrn": "",
                    "sr_no": _digits(self._next_raw_id(), 10),
                    "emi_details": "",
                    "emi_rrn": "",
                    "is_emitran": False,
                    "inserted_on": when_naive,
                    "trancurrency": "356",
                    "request_datetime": when_naive,
                    "rsp_datetime": when_naive,
                    "mcc": self.context.mcc_code,
                    "card_seq_no": "00",
                    "issuer_script": "",
                    "emitxnrefno": "",
                    "emitenure": "0",
                    "org_tran_id": "",
                    "tc": "",
                    "is_pin_entered": product["rate_card_type"] == "DEBIT",
                    "card_type_identifier": "",
                    "emicardtype": "",
                    "billnumber": invoice_nr,
                    "hash": request_hash,
                    "hosttype": "LyraHost",
                    "dccratemarkup": "",
                    "dccratemarkuptext": "",
                    "dccinverseconversionrate": "",
                    "dccconversionrate": "",
                    "dccamount": "",
                    "devicetype": "POS",
                    "dcccurrencycode": "0356",
                    "sale_amt": _digits(amount_paise, 12),
                    "p_date": when_local.date(),
                    "tran_mode": "1",
                    "source_id": "",
                    "unformatted_dccamount": "",
                    "dccratemarkupamount": "",
                    "dcccommission": "",
                    "issigned": False,
                    "crdexp": _digits(self._next_raw_id(), 4),
                    "cvv": "",
                    "ismototxn": False,
                    "accounttype": "Default",
                    "isoffersale": False,
                    "offerid": "0",
                    "offercode": "",
                    "grossamt": "",
                    "discountdeducted": "",
                },
            )
        ]

        fact_row = {
            "source_system": "CARD",
            "source_txn_id": rrn,
            "merchant_id": self.context.mid,
            "terminal_id": self.context.tid,
            "invoice_nr": invoice_nr,
            "initiated_at": when_utc,
            "completed_at": when_utc,
            "p_date": when_local.date(),
            "amount_paise": amount_paise,
            "currency": "INR",
            "status": status,
            "response_code": response_code,
            "response_desc": response_desc,
            "payment_mode": "CARD",
            "sub_mode": "POS",
            "card_network": product["network_label"],
            "card_type": product["card_type_code"],
            "raw_card_autoid": str(raw_id),
            "raw_upi_autoid": None,
            "expected_settlement_at": when_utc + dt.timedelta(days=2),
            "created_at": when_utc,
        }
        feature_row = {
            "source_system": "CARD",
            "source_txn_id": rrn,
            "merchant_id": self.context.mid,
            "terminal_id": self.context.tid,
            "invoice_nr": invoice_nr,
            "raw_card_autoid": str(raw_id),
            "raw_upi_autoid": None,
            "payment_mode": "CARD",
            "sub_mode": "POS",
            "card_network": product["network_label"],
            "card_type": product["card_type_code"],
            "status": status,
            "response_code": response_code,
            "response_desc": response_desc,
            "currency": "INR",
            "amount_paise": amount_paise,
            "amount_rupees": round(amount_paise / 100.0, 2),
            "amount_bucket": _amount_bucket(amount_rupees),
            "p_date": when_local.date(),
            "initiated_at": when_utc,
            "completed_at": when_utc,
            "expected_settlement_at": when_utc + dt.timedelta(days=2),
            "hour_of_day": when_local.hour,
            "day_of_week": when_local.weekday(),
            "is_weekend": when_local.weekday() >= 5,
            "is_night": when_local.hour < 6 or when_local.hour >= 22,
            "card_bin": product["bin"],
            "mcc": self.context.mcc_code,
            "pos_entry_mode": product["entry_mode"],
            "pos_type": "",
            "device_type": "POS",
            "account_type": "Default",
            "is_pin_entered": product["rate_card_type"] == "DEBIT",
            "is_moto_txn": False,
            "upi_app_name": None,
            "upi_channel_code": None,
            "upi_txn_type": None,
            "upi_mcc_code": None,
            "payer_vpa_domain": None,
            "payee_vpa_domain": None,
            "payer_ifsc": None,
            "payee_ifsc": None,
            "payer_bank_code": None,
            "payee_bank_code": None,
            "payer_account_type": None,
            "time_since_prev_terminal_secs": None,
            "time_since_prev_merchant_secs": None,
            "terminal_txn_count_1h": None,
            "merchant_txn_count_1h": None,
            "terminal_success_rate_1h": None,
            "merchant_success_rate_1h": None,
            "created_at": when_utc,
        }
        return GeneratedTransaction(
            payment_mode="CARD",
            status=status,
            amount_rupees=round(amount_paise / 100.0, 2),
            amount_paise=amount_paise,
            rrn=rrn,
            source_txn_id=rrn,
            mdr_rate_pct=self._resolve_mdr_rate("CARD", product["rate_network"], product["rate_card_type"]),
            raw_inserts=raw_rows,
            fact_row=fact_row,
            feature_row=feature_row,
            card_rate_network=product["rate_network"],
            card_rate_type=product["rate_card_type"],
        )

    def _maybe_build_refund(self, txn: GeneratedTransaction, when_local: dt.datetime) -> dict[str, Any] | None:
        if self.config.refund_probability <= 0:
            return None
        if txn.status != "SUCCESS":
            return None
        if self.rng.random() > self.config.refund_probability:
            return None

        when_utc = _utc(when_local)
        full_refund = self.rng.random() < 0.35
        refund_amount = txn.amount_rupees if full_refund else round(txn.amount_rupees * self.rng.uniform(0.2, 0.75), 2)
        return {
            "source_system": txn.payment_mode,
            "source_txn_id": txn.source_txn_id,
            "mid": self.context.mid,
            "tid": self.context.tid,
            "original_rrn": txn.rrn,
            "refund_amount": refund_amount,
            "refund_type": "FULL" if full_refund else "PARTIAL",
            "refund_reason": self.rng.choice(REFUND_REASONS),
            "refund_status": "PROCESSED",
            "refund_date": when_utc,
            "credit_date": when_utc + dt.timedelta(minutes=15),
            "refund_rrn": self._next_rrn(),
            "refund_auth_code": None,
            "mdr_reversed": txn.payment_mode == "CARD",
            "chargeback_id": None,
            "settlement_id": None,
            "p_date": when_local.date(),
            "created_at": when_utc,
        }

    def _maybe_build_chargeback(self, txn: GeneratedTransaction, when_local: dt.datetime) -> dict[str, Any] | None:
        if self.config.chargeback_probability <= 0:
            return None
        if txn.status != "SUCCESS":
            return None
        if self.rng.random() > self.config.chargeback_probability:
            return None

        when_utc = _utc(when_local)
        code, desc = self.rng.choice(CHARGEBACK_REASONS)
        network = "NPCI_UPI" if txn.payment_mode == "UPI" else txn.fact_row.get("card_network")
        return {
            "source_system": txn.payment_mode,
            "source_txn_id": txn.source_txn_id,
            "mid": self.context.mid,
            "tid": self.context.tid,
            "original_rrn": txn.rrn,
            "original_auth_code": None,
            "chargeback_amount": round(txn.amount_rupees * self.rng.uniform(0.4, 1.0), 2),
            "chargeback_currency": "INR",
            "chargeback_reason_code": code,
            "chargeback_reason_desc": desc,
            "card_network": network,
            "chargeback_type": "FIRST_CHARGEBACK",
            "chargeback_stage": "OPEN",
            "filed_date": when_local.date(),
            "response_due_date": when_local.date() + dt.timedelta(days=14),
            "merchant_response_date": None,
            "merchant_response_status": "PENDING",
            "resolution_date": None,
            "resolution_outcome": None,
            "debit_credit_flag": "DEBIT",
            "representment_doc_ref": None,
            "arbitration_fee": 0.0,
            "settlement_id": None,
            "p_date": when_local.date(),
            "created_at": when_utc,
            "updated_at": when_utc,
        }

    def _build_terminal_health_snapshot(self, when_local: dt.datetime) -> dict[str, Any]:
        preset = BUSINESS_PRESETS.get(self.context.profile_id, _preset_for_mcc(self.context.mcc_code))
        lat = self.context.location_latitude if self.context.location_latitude is not None else preset.latitude
        lon = self.context.location_longitude if self.context.location_longitude is not None else preset.longitude

        low_network = self.rng.random() < preset.low_network_probability
        quick_drain = self.rng.random() < preset.quick_battery_probability
        geo_drift = self.rng.random() < preset.geo_drift_probability
        battery = max(8, min(99, int(self.rng.uniform(28, 86) - (12 if quick_drain else 0))))
        ram_util = max(32, min(92, int(self.rng.uniform(42, 78) + (8 if low_network else 0))))

        lat_jitter = self.rng.uniform(-0.0008, 0.0008)
        lon_jitter = self.rng.uniform(-0.0008, 0.0008)
        if geo_drift:
            lat_jitter += self.rng.uniform(0.01, 0.03)
            lon_jitter += self.rng.uniform(0.01, 0.03)

        payload = {
            "tid": self.context.tid,
            "battery": battery,
            "ram_rom_utilization": ram_util,
            "low_network_strength": low_network,
            "quick_battery_drainage": quick_drain,
            "latitude_longitude_deviation": geo_drift,
        }

        return {
            "tid": self.context.tid,
            "serial_no": self.context.terminal_serial_no or f"{self.context.tid}-SN",
            "mid": self.context.mid,
            "param": "terminal_health",
            "vendor_id": 101,
            "created_by": 1,
            "captured_at": _utc(when_local),
            "latitude": round(lat + lat_jitter, 6),
            "longitude": round(lon + lon_jitter, 6),
            "application_list": json.dumps(["upi", "card", "settlement", "demo-live-feed"], ensure_ascii=True),
            "application_package_name": "com.acquiguru.demo",
            "application_version_code": self.context.app_version or "DEMO_LIVE_1.0",
            "printer_status": "OK" if self.context.terminal_type != "MOBILE" else "NA",
            "printer_sensor": "OK" if self.context.terminal_type != "MOBILE" else "NA",
            "chip_reader_sensor": "OK",
            "nfc_sensor": "OK",
            "sim_details": json.dumps({"sim1": "airtel", "connectivity": self.context.connectivity_type or preset.connectivity_type}, ensure_ascii=True),
            "ram_rom_utilization": ram_util,
            "battery_status": battery,
            "latitude_longitude_deviation": geo_drift,
            "quick_battery_drainage": quick_drain,
            "low_network_strength": low_network,
            "raw_payload": json.dumps(payload, ensure_ascii=True),
        }

    def _build_fraud_alerts(self, transactions: list[GeneratedTransaction], when_local: dt.datetime) -> list[dict[str, Any]]:
        alerts: list[dict[str, Any]] = []
        when_utc = _utc(when_local)

        failed_cards = [tx for tx in transactions if tx.payment_mode == "CARD" and tx.status == "FAILED" and tx.amount_rupees <= 500]
        if len(failed_cards) >= 3:
            trigger_tx = failed_cards[-1]
            alerts.append(
                {
                    "source_system": trigger_tx.payment_mode,
                    "source_txn_id": trigger_tx.source_txn_id,
                    "mid": self.context.mid,
                    "tid": self.context.tid,
                    "alert_type": "CARD_TESTING",
                    "alert_severity": "CRITICAL",
                    "rule_triggered": "Multiple low-value declines in 2 min",
                    "rule_id": "CT-004",
                    "alert_status": "OPEN",
                    "investigation_notes": "Auto-generated demo alert from repeated low-value card declines.",
                    "false_positive_flag": False,
                    "action_taken": "REVIEW",
                    "reported_to_regulator": False,
                    "sar_reference": None,
                    "alert_timestamp": when_utc,
                    "closed_timestamp": None,
                    "closed_by": None,
                    "p_date": when_local.date(),
                    "created_at": when_utc,
                }
            )

        high_ticket = [tx for tx in transactions if tx.amount_rupees >= max(2000.0, self.context.expected_avg_ticket_size * 3)]
        if high_ticket and self.rng.random() <= self.config.fraud_alert_probability:
            trigger_tx = max(high_ticket, key=lambda tx: tx.amount_rupees)
            alerts.append(
                {
                    "source_system": trigger_tx.payment_mode,
                    "source_txn_id": trigger_tx.source_txn_id,
                    "mid": self.context.mid,
                    "tid": self.context.tid,
                    "alert_type": "AMOUNT",
                    "alert_severity": "HIGH",
                    "rule_triggered": "Single txn > 3x avg ticket size",
                    "rule_id": "AMT-002",
                    "alert_status": "OPEN",
                    "investigation_notes": f"Auto-generated for a {trigger_tx.amount_rupees:.0f} INR ticket in the live feed.",
                    "false_positive_flag": False,
                    "action_taken": "MONITOR",
                    "reported_to_regulator": False,
                    "sar_reference": None,
                    "alert_timestamp": when_utc,
                    "closed_timestamp": None,
                    "closed_by": None,
                    "p_date": when_local.date(),
                    "created_at": when_utc,
                }
            )

        late_night = when_local.hour < 5 or when_local.hour >= 23
        if late_night and self.context.profile_id not in {"airport_parking"} and self.rng.random() <= self.config.fraud_alert_probability * 0.4:
            trigger_tx = transactions[-1]
            alerts.append(
                {
                    "source_system": trigger_tx.payment_mode,
                    "source_txn_id": trigger_tx.source_txn_id,
                    "mid": self.context.mid,
                    "tid": self.context.tid,
                    "alert_type": "PATTERN",
                    "alert_severity": "LOW",
                    "rule_triggered": "Unusual late-night txn pattern",
                    "rule_id": "PAT-003",
                    "alert_status": "OPEN",
                    "investigation_notes": "Auto-generated late-night pattern alert for non-24x7 business profile.",
                    "false_positive_flag": False,
                    "action_taken": "MONITOR",
                    "reported_to_regulator": False,
                    "sar_reference": None,
                    "alert_timestamp": when_utc,
                    "closed_timestamp": None,
                    "closed_by": None,
                    "p_date": when_local.date(),
                    "created_at": when_utc,
                }
            )

        return alerts

    def _build_fee_ledger_rows(self, successful_transactions: list[GeneratedTransaction], when_local: dt.datetime) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for txn in successful_transactions:
            rate = self._resolve_rate_card(txn.payment_mode, txn.card_rate_network or "", txn.card_rate_type or "")
            txn_amount = round(txn.amount_rupees, 2)
            mdr_amount = round(txn_amount * rate["mdr_percentage"] / 100.0, 2)
            interchange_fee = round(txn_amount * rate["interchange_rate"] / 100.0, 2)
            acquirer_fee = round(txn_amount * rate["acquirer_margin"] / 100.0, 2)
            scheme_fee = round(txn_amount * rate["scheme_fee"] / 100.0, 2)
            gst_amount = round(mdr_amount * rate["gst_on_mdr_pct"] / 100.0, 2) if mdr_amount else 0.0
            net_fee = round(mdr_amount + gst_amount, 2)

            rows.append(
                {
                    "source_system": txn.payment_mode,
                    "source_txn_id": txn.source_txn_id,
                    "mid": self.context.mid,
                    "tid": self.context.tid,
                    "txn_amount": txn_amount,
                    "mdr_amount": mdr_amount,
                    "interchange_fee": interchange_fee,
                    "acquirer_fee": acquirer_fee,
                    "scheme_assessment_fee": scheme_fee,
                    "gst_amount": gst_amount,
                    "net_fee": net_fee,
                    "mdr_rate_id": rate["mdr_id"],
                    "fee_debit_date": when_local.date() + dt.timedelta(days=1),
                    "invoice_number": txn.fact_row.get("invoice_nr"),
                    "p_date": when_local.date(),
                    "created_at": _utc(when_local),
                }
            )
        return rows

    def generate_batch(self, now_local: dt.datetime | None = None) -> GeneratedBatch:
        now_local = now_local or dt.datetime.now(IST)
        self._ensure_rate_table_for_context(self._select_context())
        batch_size = self.rng.randint(self.config.batch_min, self.config.batch_max)
        window_seconds = max(self.config.interval_seconds, float(batch_size))
        batch_start = now_local - dt.timedelta(seconds=window_seconds)
        offsets = sorted(self.rng.uniform(0.0, window_seconds) for _ in range(batch_size))

        transactions: list[GeneratedTransaction] = []
        refunds: list[dict[str, Any]] = []
        chargebacks: list[dict[str, Any]] = []
        terminal_health_snapshots: list[dict[str, Any]] = []
        fraud_alerts: list[dict[str, Any]] = []
        fee_ledger_rows: list[dict[str, Any]] = []

        for offset in offsets:
            when_local_txn = batch_start + dt.timedelta(seconds=offset)
            payment_mode = "UPI" if self.rng.random() <= self.config.upi_share else "CARD"
            txn = self._build_upi_transaction(when_local_txn) if payment_mode == "UPI" else self._build_card_transaction(when_local_txn)
            transactions.append(txn)

            refund = self._maybe_build_refund(txn, when_local_txn + dt.timedelta(minutes=3))
            if refund:
                refunds.append(refund)
            chargeback = self._maybe_build_chargeback(txn, when_local_txn + dt.timedelta(minutes=10))
            if chargeback:
                chargebacks.append(chargeback)

        settlements: list[dict[str, Any]] = []
        should_emit_settlements = self.config.settlement_every_batches > 0 and (
            self.config.once or self._batch_counter % self.config.settlement_every_batches == 0
        )
        if should_emit_settlements:
            successful = [txn for txn in transactions if txn.status == "SUCCESS"]
            settlements = build_settlement_rows(
                successful,
                context=self.context,
                when_local=now_local,
                batch_number=self._batch_counter,
            )
        else:
            successful = [txn for txn in transactions if txn.status == "SUCCESS"]

        if self.config.fee_ledger_enabled:
            fee_ledger_rows = self._build_fee_ledger_rows(successful, now_local)

        should_emit_health = self.config.terminal_health_every_batches > 0 and (
            self.config.once or self._batch_counter % self.config.terminal_health_every_batches == 0
        )
        if should_emit_health:
            terminal_health_snapshots.append(self._build_terminal_health_snapshot(now_local))

        if self.config.fraud_alert_probability > 0:
            fraud_alerts = self._build_fraud_alerts(transactions, now_local)

        return GeneratedBatch(
            transactions=transactions,
            settlements=settlements,
            refunds=refunds,
            chargebacks=chargebacks,
            terminal_health_snapshots=terminal_health_snapshots,
            fraud_alerts=fraud_alerts,
            fee_ledger_rows=fee_ledger_rows,
        )

    def _insert_row(self, conn: Connection, table: str, row: dict[str, Any], returning: str | None = None) -> Any:
        columns = list(row.keys())
        values = ", ".join(f":{col}" for col in columns)
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({values})"
        if returning:
            sql += f" RETURNING {returning}"
        result = conn.execute(text(sql), row)
        if returning:
            return result.scalar_one()
        return None

    def _update_terminal(self, conn: Connection, latest_p_date: dt.date, updated_at: dt.datetime) -> None:
        conn.execute(
            text(
                """
                UPDATE terminals
                SET last_txn_date = :last_txn_date,
                    updated_at = :updated_at
                WHERE tid = :tid
                """
            ),
            {"last_txn_date": latest_p_date, "updated_at": updated_at, "tid": self.context.tid},
        )

    def persist_batch(self, batch: GeneratedBatch) -> dict[str, Any]:
        latest_p_date = max(txn.fact_row["p_date"] for txn in batch.transactions)
        latest_created_at = max(txn.fact_row["created_at"] for txn in batch.transactions)

        with self.engine.begin() as conn:
            for txn in batch.transactions:
                for raw_insert in txn.raw_inserts:
                    self._insert_row(conn, raw_insert.table, raw_insert.row)
                fact_id = self._insert_row(conn, "transaction_fact", txn.fact_row, returning="transaction_fact_id")
                feature_row = dict(txn.feature_row)
                feature_row["transaction_fact_id"] = fact_id
                self._insert_row(conn, "transaction_features", feature_row)

            for row in batch.settlements:
                self._insert_row(conn, "settlements", row)
            for row in batch.refunds:
                self._insert_row(conn, "refunds", row)
            for row in batch.chargebacks:
                self._insert_row(conn, "chargebacks", row)
            for row in batch.terminal_health_snapshots:
                self._insert_row(conn, "terminal_health_snapshots", row)
            for row in batch.fraud_alerts:
                self._insert_row(conn, "fraud_alerts", row)
            for row in batch.fee_ledger_rows:
                self._insert_row(conn, "fee_ledger", row)

            self._update_terminal(conn, latest_p_date, latest_created_at)

        return self._summarize_batch(batch)

    def _summarize_batch(self, batch: GeneratedBatch) -> dict[str, Any]:
        mode_counts: dict[str, int] = {}
        status_counts: dict[str, int] = {}
        amount_by_mode: dict[str, float] = {}
        raw_rows = 0
        for txn in batch.transactions:
            mode_counts[txn.payment_mode] = mode_counts.get(txn.payment_mode, 0) + 1
            status_counts[txn.status] = status_counts.get(txn.status, 0) + 1
            amount_by_mode[txn.payment_mode] = round(amount_by_mode.get(txn.payment_mode, 0.0) + txn.amount_rupees, 2)
            raw_rows += len(txn.raw_inserts)
        return {
            "merchant_id": self.context.mid,
            "terminal_id": self.context.tid,
            "transactions_inserted": len(batch.transactions),
            "raw_rows_inserted": raw_rows,
            "settlements_inserted": len(batch.settlements),
            "refunds_inserted": len(batch.refunds),
            "chargebacks_inserted": len(batch.chargebacks),
            "terminal_health_inserted": len(batch.terminal_health_snapshots),
            "fraud_alerts_inserted": len(batch.fraud_alerts),
            "fee_ledger_inserted": len(batch.fee_ledger_rows),
            "status_breakdown": status_counts,
            "payment_mode_breakdown": mode_counts,
            "amount_inr_by_mode": amount_by_mode,
        }

    def run(self) -> None:
        self.install_signal_handlers()
        logger.info(
            "Starting demo activity generator for MID=%s TID=%s batch=%s-%s interval=%.1fs dry_run=%s",
            self.context.mid,
            self.context.tid,
            self.config.batch_min,
            self.config.batch_max,
            self.config.interval_seconds,
            self.config.dry_run,
        )
        batches_run = 0

        while self._continue:
            self._batch_counter += 1
            batch = self.generate_batch()
            summary = self._summarize_batch(batch) if self.config.dry_run else self.persist_batch(batch)
            logger.info("Batch %s: %s", self._batch_counter, json.dumps(summary, ensure_ascii=True, default=str))

            batches_run += 1
            if self.config.once or (self.config.max_batches > 0 and batches_run >= self.config.max_batches):
                break
            time.sleep(max(self.config.interval_seconds, 0.1))


class PsqlClient:
    def __init__(self, database_url: str):
        self.database_url = database_url

    def _run(self, *, sql: str, csv_output: bool = False, input_sql: str | None = None) -> str:
        cmd = ["psql", self.database_url, "-v", "ON_ERROR_STOP=1"]
        if csv_output:
            cmd.append("--csv")
        if input_sql is None:
            cmd.extend(["-c", sql])
        proc = subprocess.run(
            cmd,
            input=input_sql,
            text=True,
            capture_output=True,
            check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "psql command failed")
        return proc.stdout

    def _query_csv(self, sql: str) -> list[dict[str, str]]:
        output = self._run(sql=sql, csv_output=True)
        return list(csv.DictReader(io.StringIO(output)))

    @staticmethod
    def _literal(value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, dt.datetime):
            return "'" + value.isoformat(sep=" ") + "'"
        if isinstance(value, dt.date):
            return "'" + value.isoformat() + "'"
        return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"

    def insert_sql(self, table: str, row: dict[str, Any]) -> str:
        cols = ", ".join(row.keys())
        vals = ", ".join(self._literal(value) for value in row.values())
        return f"INSERT INTO {table} ({cols}) VALUES ({vals});"

    def load_context(self, merchant_id: str | None, terminal_id: str | None) -> MerchantContext:
        mid_literal = self._literal(merchant_id)
        tid_literal = self._literal(terminal_id)
        sql = f"""
            SELECT
              m.mid,
              t.tid,
              COALESCE(m.merchant_trade_name, m.merchant_legal_name, m.mid) AS merchant_trade_name,
              COALESCE(m.merchant_legal_name, m.merchant_trade_name, m.mid) AS merchant_legal_name,
              COALESCE(m.business_city, '') AS business_city,
              COALESCE(m.business_state, '') AS business_state,
              COALESCE(m.mcc_code, '7523') AS mcc_code,
              COALESCE(m.expected_avg_ticket_size, 550.0) AS expected_avg_ticket_size,
              COALESCE(c.max_transaction_limit, 100000.0) AS max_transaction_limit,
              COALESCE(t.vpa, 'merchant@icici') AS vpa,
              COALESCE(t.terminal_make, '') AS terminal_make,
              COALESCE(t.terminal_model, '') AS terminal_model,
              COALESCE(t.app_version, '') AS app_version,
              COALESCE(t.terminal_serial_no, '') AS terminal_serial_no,
              t.location_latitude,
              t.location_longitude,
              COALESCE(t.connectivity_type, '') AS connectivity_type,
              COALESCE(t.terminal_type, '') AS terminal_type,
              mba.account_id AS primary_bank_account_id
            FROM merchants m
            JOIN terminals t
              ON t.mid = m.mid
            LEFT JOIN mcc_codes c
              ON c.mcc_code = m.mcc_code
            LEFT JOIN merchant_bank_accounts mba
              ON mba.mid = m.mid
             AND COALESCE(mba.is_primary, false) = true
            WHERE ({mid_literal} IS NULL OR m.mid = {mid_literal})
              AND ({tid_literal} IS NULL OR t.tid = {tid_literal})
            ORDER BY m.mid, t.tid
            LIMIT 1
        """
        rows = self._query_csv(sql)
        if not rows:
            raise RuntimeError("No merchant/terminal pair found in the database.")
        return _context_from_row(rows[0])

    def load_mdr_rates(self, mid: str) -> dict[tuple[str, str, str], dict[str, Any]]:
        sql = f"""
            SELECT payment_mode, COALESCE(card_network, '') AS card_network,
                   COALESCE(card_type, '') AS card_type,
                   mdr_id,
                   COALESCE(mdr_percentage, 0) AS mdr_percentage,
                   COALESCE(gst_on_mdr_pct, 18) AS gst_on_mdr_pct,
                   COALESCE(interchange_rate, 0) AS interchange_rate,
                   COALESCE(acquirer_margin, 0) AS acquirer_margin,
                   COALESCE(scheme_fee, 0) AS scheme_fee
            FROM mdr_rates
            WHERE mid = {self._literal(mid)}
        """
        rows = self._query_csv(sql)
        out: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in rows:
            key = (
                str(row["payment_mode"] or "").upper(),
                str(row["card_network"] or "").upper(),
                str(row["card_type"] or "").upper(),
            )
            out[key] = {
                "mdr_id": int(row["mdr_id"]) if row["mdr_id"] else None,
                "mdr_percentage": float(row["mdr_percentage"] or 0.0),
                "gst_on_mdr_pct": float(row["gst_on_mdr_pct"] or 18.0),
                "interchange_rate": float(row["interchange_rate"] or 0.0),
                "acquirer_margin": float(row["acquirer_margin"] or 0.0),
                "scheme_fee": float(row["scheme_fee"] or 0.0),
            }
        return out

    def load_counters(self) -> tuple[int, int, int, int]:
        sql = """
            SELECT
              GREATEST(
                COALESCE((SELECT MAX(autoid) FROM raw_upi_transactions), 0),
                COALESCE((SELECT MAX(autoid) FROM raw_upi_qr_records), 0),
                COALESCE((SELECT MAX(autoid) FROM raw_upi_mqtt_logs), 0),
                COALESCE((SELECT MAX(auto_id) FROM raw_upi_notifications), 0),
                COALESCE((SELECT MAX(id) FROM raw_upi_callback_logs), 0),
                COALESCE((SELECT MAX(autoid) FROM raw_card_transactions), 0),
                0
              ) AS max_raw_id,
              GREATEST(
                COALESCE((SELECT MAX(CAST(rrn AS bigint)) FROM raw_upi_transactions WHERE rrn ~ '^[0-9]+$'), 0),
                COALESCE((SELECT MAX(CAST(rrn AS bigint)) FROM raw_card_transactions WHERE rrn ~ '^[0-9]+$'), 0),
                0
              ) AS max_rrn,
              COALESCE(
                (SELECT MAX(CAST(invoice_nr AS bigint))
                 FROM transaction_fact
                 WHERE invoice_nr ~ '^[0-9]+$'),
                0
              ) AS max_invoice,
              GREATEST(
                COALESCE((SELECT MAX(CAST(stan AS bigint)) FROM raw_upi_transactions WHERE stan ~ '^[0-9]+$'), 0),
                COALESCE((SELECT MAX(CAST(stan AS bigint)) FROM raw_card_transactions WHERE stan ~ '^[0-9]+$'), 0),
                0
              ) AS max_stan
        """
        row = self._query_csv(sql)[0]
        return (
            int(row["max_raw_id"] or 0),
            int(row["max_rrn"] or 0),
            int(row["max_invoice"] or 0),
            int(row["max_stan"] or 0),
        )

    def next_transaction_fact_ids(self, count: int) -> list[int]:
        sql = f"""
            SELECT nextval('transaction_fact_transaction_fact_id_seq') AS transaction_fact_id
            FROM generate_series(1, {count})
        """
        return [int(row["transaction_fact_id"]) for row in self._query_csv(sql)]

    def persist_batch(self, batch: GeneratedBatch, *, context: MerchantContext, summary: dict[str, Any]) -> dict[str, Any]:
        fact_ids = self.next_transaction_fact_ids(len(batch.transactions))
        latest_p_date = max(txn.fact_row["p_date"] for txn in batch.transactions)
        latest_created_at = max(txn.fact_row["created_at"] for txn in batch.transactions)

        statements = ["BEGIN;"]
        for fact_id, txn in zip(fact_ids, batch.transactions):
            for raw_insert in txn.raw_inserts:
                statements.append(self.insert_sql(raw_insert.table, raw_insert.row))
            statements.append(self.insert_sql("transaction_fact", {"transaction_fact_id": fact_id, **txn.fact_row}))
            statements.append(self.insert_sql("transaction_features", {"transaction_fact_id": fact_id, **txn.feature_row}))

        for row in batch.settlements:
            statements.append(self.insert_sql("settlements", row))
        for row in batch.refunds:
            statements.append(self.insert_sql("refunds", row))
        for row in batch.chargebacks:
            statements.append(self.insert_sql("chargebacks", row))
        for row in batch.terminal_health_snapshots:
            statements.append(self.insert_sql("terminal_health_snapshots", row))
        for row in batch.fraud_alerts:
            statements.append(self.insert_sql("fraud_alerts", row))
        for row in batch.fee_ledger_rows:
            statements.append(self.insert_sql("fee_ledger", row))

        statements.append(
            f"""
            UPDATE terminals
            SET last_txn_date = {self._literal(latest_p_date)},
                updated_at = {self._literal(latest_created_at)}
            WHERE tid = {self._literal(context.tid)};
            """
        )
        statements.append("COMMIT;")

        self._run(sql="", input_sql="\n".join(statements))
        return summary


STATE_CODES = {
    "DELHI": "07",
    "KARNATAKA": "29",
    "MAHARASHTRA": "27",
    "TELANGANA": "36",
}


def _profile_prefix(profile_id: str) -> str:
    parts = profile_id.split("_")
    return "".join(part[0].upper() for part in parts)[:2].ljust(2, "X")


def _generate_pan(rng: random.Random) -> str:
    letters = "".join(rng.choice(string.ascii_uppercase) for _ in range(5))
    digits = "".join(rng.choice(string.digits) for _ in range(4))
    suffix = rng.choice(string.ascii_uppercase)
    return f"{letters}{digits}{suffix}"


def _generate_gst(state: str, pan: str, entity_index: int) -> str:
    state_code = STATE_CODES.get(state.upper(), "29")
    return f"{state_code}{pan}{entity_index % 9 + 1}Z5"


def _build_mdr_rows(mid: str, now_utc: dt.datetime) -> list[dict[str, Any]]:
    rows = [
        ("CARD", "VISA", "DEBIT", "DOMESTIC", 0.4, 0.15, 0.20, 0.05),
        ("CARD", "MASTERCARD", "DEBIT", "DOMESTIC", 0.4, 0.15, 0.20, 0.05),
        ("CARD", "RUPAY", "DEBIT", "DOMESTIC", 0.0, 0.0, 0.0, 0.0),
        ("CARD", "VISA", "CREDIT", "DOMESTIC", 1.8, 1.2, 0.4, 0.2),
        ("CARD", "MASTERCARD", "CREDIT", "DOMESTIC", 1.8, 1.2, 0.4, 0.2),
        ("CARD", "RUPAY", "CREDIT", "DOMESTIC", 1.5, 1.0, 0.35, 0.15),
        ("CARD", "VISA", "CREDIT", "INTERNATIONAL", 2.5, 1.6, 0.6, 0.3),
        ("CARD", "MASTERCARD", "CREDIT", "INTERNATIONAL", 2.5, 1.6, 0.6, 0.3),
        ("CARD", "AMEX", "CREDIT", "DOMESTIC", 2.0, 1.5, 0.3, 0.2),
        ("UPI", None, None, "DOMESTIC", 0.0, 0.0, 0.0, 0.0),
        ("QR", None, None, "DOMESTIC", 0.0, 0.0, 0.0, 0.0),
    ]
    out: list[dict[str, Any]] = []
    effective_from = dt.date.today() - dt.timedelta(days=90)
    for payment_mode, card_network, card_type, txn_type, mdr_pct, interchange, acquirer, scheme_fee in rows:
        out.append(
            {
                "mid": mid,
                "payment_mode": payment_mode,
                "card_network": card_network,
                "card_type": card_type,
                "transaction_type": txn_type,
                "mdr_percentage": mdr_pct,
                "mdr_flat_fee": None,
                "mdr_cap": None,
                "interchange_rate": interchange,
                "interchange_fee_fixed": 0.0,
                "acquirer_margin": acquirer,
                "scheme_fee": scheme_fee,
                "scheme_fee_fixed": 0.0,
                "gst_on_mdr_pct": 18.0,
                "effective_from": effective_from,
                "effective_to": None,
                "turnover_slab_min": None,
                "turnover_slab_max": None,
                "special_rate_flag": False,
                "approved_by": "DEMO_ACTIVITY_GENERATOR",
                "created_at": now_utc,
            }
        )
    return out


def _parse_portfolio_specs(config: GeneratorConfig) -> list[PortfolioSpec]:
    if config.portfolio:
        specs: list[PortfolioSpec] = []
        for item in config.portfolio.split(","):
            token = item.strip()
            if not token:
                continue
            if ":" in token:
                profile_id, count_text = token.split(":", 1)
                count = int(count_text)
            else:
                profile_id, count = token, 1
            specs.append(PortfolioSpec(profile_id=profile_id.strip(), merchant_count=max(1, count)))
        return specs
    if config.business_profile:
        return [PortfolioSpec(profile_id=config.business_profile, merchant_count=max(1, config.merchant_count))]
    return []


def provision_demo_portfolio(psql: PsqlClient, config: GeneratorConfig) -> list[MerchantContext]:
    specs = _parse_portfolio_specs(config)
    if not specs:
        return []

    rng = random.Random(config.seed)
    now_local = dt.datetime.now(IST)
    now_utc = _utc(now_local)
    statements = ["BEGIN;"]
    created_pairs: list[tuple[str, str]] = []

    for spec in specs:
        preset = _preset_for_profile(spec.profile_id)
        terminal_count = config.terminal_count if config.terminal_count > 0 else preset.default_terminal_count
        for merchant_idx in range(spec.merchant_count):
            mid = "9" + _digits(uuid.uuid4().int, 14)
            pan = _generate_pan(rng)
            trade_name = f"{preset.trade_name_prefix} {merchant_idx + 1:02d}"
            legal_name = f"{preset.legal_name_prefix} {merchant_idx + 1:02d} Private Limited"
            created_pairs.extend(
                [
                    (
                        mid,
                        f"{_profile_prefix(preset.profile_id)}{_digits(uuid.uuid4().int, 6)}",
                    )
                    for _ in range(terminal_count)
                ]
            )

            statements.append(
                psql.insert_sql(
                    "merchants",
                    {
                        "mid": mid,
                        "merchant_legal_name": legal_name,
                        "merchant_trade_name": trade_name,
                        "merchant_type": preset.merchant_type,
                        "business_registration_no": f"U{_digits(uuid.uuid4().int, 8)}DL20{_digits(uuid.uuid4().int, 6)}",
                        "gst_number": _generate_gst(preset.state, pan, merchant_idx + 1),
                        "pan_number": pan,
                        "business_address": f"Demo {preset.trade_name_prefix} Outlet, {preset.city}",
                        "business_city": preset.city,
                        "business_state": preset.state,
                        "business_pincode": preset.pincode,
                        "business_country": "IN",
                        "mcc_code": preset.mcc_code,
                        "merchant_risk_category": "LOW",
                        "onboarding_date": now_local.date() - dt.timedelta(days=60),
                        "activation_date": now_local.date() - dt.timedelta(days=45),
                        "merchant_status": "ACTIVE",
                        "annual_turnover": preset.annual_turnover,
                        "expected_monthly_volume": preset.expected_monthly_volume,
                        "expected_avg_ticket_size": preset.expected_avg_ticket_size,
                        "business_vintage_years": 4,
                        "website_url": f"https://demo-{preset.profile_id}-{merchant_idx + 1}.example.com",
                        "nature_of_business": preset.nature_of_business,
                        "aggregator_id": "DEMO",
                        "sales_officer_id": "ACQ-DEMO",
                        "parent_merchant_id": None,
                        "franchise_flag": False,
                        "created_at": now_utc,
                        "updated_at": now_utc,
                    },
                )
            )
            statements.append(
                psql.insert_sql(
                    "merchant_bank_accounts",
                    {
                        "mid": mid,
                        "account_number": _digits(uuid.uuid4().int, 14),
                        "account_holder_name": legal_name,
                        "bank_name": "ICICI Bank",
                        "ifsc_code": "ICIC0000101",
                        "branch_name": preset.city,
                        "account_type": "CURRENT",
                        "is_primary": True,
                        "penny_drop_verified": True,
                        "penny_drop_date": now_local.date() - dt.timedelta(days=44),
                        "account_status": "ACTIVE",
                        "nodal_account_id": None,
                        "created_at": now_utc,
                        "updated_at": now_utc,
                    },
                )
            )

            for terminal_idx in range(terminal_count):
                tid = created_pairs[-terminal_count + terminal_idx][1]
                lat = round(preset.latitude + rng.uniform(-0.01, 0.01), 6)
                lon = round(preset.longitude + rng.uniform(-0.01, 0.01), 6)
                statements.append(
                    psql.insert_sql(
                        "terminals",
                        {
                            "tid": tid,
                            "mid": mid,
                            "terminal_serial_no": f"{tid}-SN",
                            "terminal_make": preset.terminal_make,
                            "terminal_model": preset.terminal_model,
                            "terminal_type": preset.terminal_type,
                            "terminal_status": "ACTIVE",
                            "sim_number": "8991100200800000001",
                            "connectivity_type": preset.connectivity_type,
                            "firmware_version": "v3.2.1",
                            "app_version": "DEMO_LIVE_1.0",
                            "deployment_date": now_local.date() - dt.timedelta(days=40),
                            "last_txn_date": None,
                            "location_address": f"Demo terminal {terminal_idx + 1}, {preset.city}",
                            "location_latitude": lat,
                            "location_longitude": lon,
                            "key_injection_date": now_local.date() - dt.timedelta(days=42),
                            "tamper_status": "NORMAL",
                            "maintenance_due_date": now_local.date() + dt.timedelta(days=180),
                            "vpa": f"{tid.lower()}@icici",
                            "created_at": now_utc,
                            "updated_at": now_utc,
                        },
                    )
                )

            for row in _build_mdr_rows(mid, now_utc):
                statements.append(psql.insert_sql("mdr_rates", row))

            statements.append(
                psql.insert_sql(
                    "merchant_risk_profiles",
                    {
                        "mid": mid,
                        "assessment_date": now_local.date(),
                        "risk_score": 15.0 if preset.profile_id != "big_merchant" else 22.0,
                        "risk_category": "LOW" if preset.profile_id != "big_merchant" else "MEDIUM",
                        "chargeback_ratio": 0.0005,
                        "fraud_ratio": 0.0002,
                        "avg_ticket_deviation": 12.5,
                        "velocity_score": 18.0,
                        "dormancy_flag": False,
                        "suspicious_activity_flag": False,
                        "pci_dss_compliance": "COMPLIANT",
                        "risk_mitigation_actions": "Daily automated monitoring",
                        "reserve_percentage": 0.0,
                        "reserve_amount": 0.0,
                        "reviewed_by": "DEMO_RISK_ENGINE",
                        "next_review_date": now_local.date() + dt.timedelta(days=90),
                        "created_at": now_utc,
                    },
                )
            )

    statements.append("COMMIT;")
    psql._run(sql="", input_sql="\n".join(statements))
    return [psql.load_context(mid, tid) for mid, tid in created_pairs]


class PsqlDemoActivityGenerator(DemoActivityGenerator):
    def __init__(self, config: GeneratorConfig, contexts: list[MerchantContext] | None = None):
        self.engine = None
        self.config = config
        self.rng = random.Random(config.seed)
        self.psql = PsqlClient(config.database_url)
        self.contexts = contexts or [self.psql.load_context(config.merchant_id, config.terminal_id)]
        self.context = self.contexts[0]
        self.mdr_rates = self.psql.load_mdr_rates(self.context.mid)
        self._raw_id, self._rrn_counter, self._invoice_counter, self._stan_counter = self.psql.load_counters()
        self._continue = True
        self._batch_counter = 0

    def _ensure_rate_table_for_context(self, context: MerchantContext) -> None:
        if self.context.mid != context.mid:
            self.context = context
            self.mdr_rates = self.psql.load_mdr_rates(context.mid)

    def persist_batch(self, batch: GeneratedBatch) -> dict[str, Any]:
        return self.psql.persist_batch(
            batch,
            context=self.context,
            summary=self._summarize_batch(batch),
        )


def _bounded_probability(value: float, name: str) -> float:
    if value < 0 or value > 1:
        raise ValueError(f"{name} must be between 0 and 1.")
    return value


def parse_args() -> GeneratorConfig:
    parser = argparse.ArgumentParser(
        description="Continuously insert synthetic live payment activity into the demo Postgres database.",
    )
    parser.add_argument("--database-url", default=Config.DATABASE_URL, help="Postgres SQLAlchemy URL.")
    parser.add_argument("--merchant-id", help="Merchant MID to target. Defaults to the first merchant.")
    parser.add_argument("--terminal-id", help="Terminal ID to target. Defaults to the first terminal for the merchant.")
    parser.add_argument("--interval-seconds", type=float, default=5.0, help="Seconds between write batches.")
    parser.add_argument("--batch-min", type=int, default=2, help="Minimum transactions per batch.")
    parser.add_argument("--batch-max", type=int, default=5, help="Maximum transactions per batch.")
    parser.add_argument("--upi-share", type=float, default=0.8, help="Share of UPI transactions in each batch.")
    parser.add_argument("--upi-success-rate", type=float, default=0.97, help="UPI success probability.")
    parser.add_argument("--card-success-rate", type=float, default=0.94, help="Card success probability.")
    parser.add_argument(
        "--settlement-every-batches",
        type=int,
        default=4,
        help="Insert settlement rows every N batches. Use 0 to disable settlements.",
    )
    parser.add_argument(
        "--refund-probability",
        type=float,
        default=0.0,
        help="Probability of attaching a refund row to a successful transaction.",
    )
    parser.add_argument(
        "--chargeback-probability",
        type=float,
        default=0.0,
        help="Probability of attaching a chargeback row to a successful transaction.",
    )
    parser.add_argument(
        "--terminal-health-every-batches",
        type=int,
        default=3,
        help="Insert terminal health snapshots every N batches. Use 0 to disable.",
    )
    parser.add_argument(
        "--fraud-alert-probability",
        type=float,
        default=0.05,
        help="Probability multiplier for fraud alert generation. Use 0 to disable.",
    )
    parser.add_argument(
        "--disable-fee-ledger",
        action="store_true",
        help="Skip fee_ledger inserts.",
    )
    parser.add_argument(
        "--business-profile",
        choices=sorted(BUSINESS_PRESETS.keys()),
        help="Provision and target a single business profile before generating activity.",
    )
    parser.add_argument(
        "--portfolio",
        help="Provision multiple business profiles, e.g. grocery_store:2,petrol_pump:1,cloud_kitchen:1,big_merchant:1",
    )
    parser.add_argument(
        "--merchant-count",
        type=int,
        default=1,
        help="When using --business-profile, create this many merchants.",
    )
    parser.add_argument(
        "--terminal-count",
        type=int,
        default=0,
        help="Override the default terminals per provisioned merchant. 0 keeps the preset default.",
    )
    parser.add_argument(
        "--provision-only",
        action="store_true",
        help="Only create the merchant/terminal master data and exit.",
    )
    parser.add_argument("--max-batches", type=int, default=0, help="Stop after N batches. 0 means run forever.")
    parser.add_argument("--seed", type=int, default=None, help="Optional random seed for repeatable output.")
    parser.add_argument("--once", action="store_true", help="Generate a single batch and exit.")
    parser.add_argument("--dry-run", action="store_true", help="Generate payloads without inserting them.")
    args = parser.parse_args()

    if args.batch_min <= 0 or args.batch_max <= 0:
        raise ValueError("batch-min and batch-max must be positive.")
    if args.batch_min > args.batch_max:
        raise ValueError("batch-min must be <= batch-max.")
    if args.interval_seconds <= 0:
        raise ValueError("interval-seconds must be > 0.")
    if args.merchant_count <= 0:
        raise ValueError("merchant-count must be positive.")
    if args.terminal_count < 0:
        raise ValueError("terminal-count must be >= 0.")
    if args.portfolio and args.business_profile:
        raise ValueError("Use either --business-profile or --portfolio, not both.")

    return GeneratorConfig(
        database_url=str(args.database_url),
        merchant_id=args.merchant_id,
        terminal_id=args.terminal_id,
        interval_seconds=float(args.interval_seconds),
        batch_min=int(args.batch_min),
        batch_max=int(args.batch_max),
        upi_share=_bounded_probability(float(args.upi_share), "upi-share"),
        upi_success_rate=_bounded_probability(float(args.upi_success_rate), "upi-success-rate"),
        card_success_rate=_bounded_probability(float(args.card_success_rate), "card-success-rate"),
        settlement_every_batches=max(0, int(args.settlement_every_batches)),
        refund_probability=_bounded_probability(float(args.refund_probability), "refund-probability"),
        chargeback_probability=_bounded_probability(float(args.chargeback_probability), "chargeback-probability"),
        terminal_health_every_batches=max(0, int(args.terminal_health_every_batches)),
        fraud_alert_probability=_bounded_probability(float(args.fraud_alert_probability), "fraud-alert-probability"),
        fee_ledger_enabled=not bool(args.disable_fee_ledger),
        business_profile=args.business_profile,
        portfolio=args.portfolio,
        merchant_count=max(1, int(args.merchant_count)),
        terminal_count=max(0, int(args.terminal_count)),
        provision_only=bool(args.provision_only),
        max_batches=max(0, int(args.max_batches)),
        seed=args.seed,
        once=bool(args.once),
        dry_run=bool(args.dry_run),
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = parse_args()
    contexts: list[MerchantContext] | None = None

    if config.business_profile or config.portfolio:
        psql = PsqlClient(config.database_url)
        contexts = provision_demo_portfolio(psql, config)
        logger.info(
            "Provisioned %s merchant-terminal target(s): %s",
            len(contexts),
            json.dumps(
                [{"mid": ctx.mid, "tid": ctx.tid, "profile_id": ctx.profile_id} for ctx in contexts],
                ensure_ascii=True,
            ),
        )
        if config.provision_only:
            return

    try:
        engine = create_engine(config.database_url, future=True)
        generator: DemoActivityGenerator = DemoActivityGenerator(engine, config, contexts=contexts)
    except ModuleNotFoundError as exc:
        logger.warning("Python Postgres driver unavailable (%s); falling back to psql CLI.", exc)
        generator = PsqlDemoActivityGenerator(config, contexts=contexts)
    generator.run()


if __name__ == "__main__":
    main()
