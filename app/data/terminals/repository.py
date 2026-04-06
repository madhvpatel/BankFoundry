from __future__ import annotations

import datetime as dt
import math
from collections import Counter
from typing import Any

from sqlalchemy import text

from app.data.merchant_ops import repository as merchant_ops_repository


def _as_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    lowered = str(value).strip().lower()
    if lowered in {"1", "true", "t", "yes", "y"}:
        return True
    if lowered in {"0", "false", "f", "no", "n"}:
        return False
    return None


def _parse_timestamp(value: Any) -> dt.datetime | None:
    if isinstance(value, dt.datetime):
        return value
    text_value = str(value or "").strip()
    if not text_value:
        return None
    try:
        return dt.datetime.fromisoformat(text_value.replace("Z", "+00:00"))
    except Exception:
        return None


def _scope_terminals(engine: Any, *, merchant_id: str, terminal_id: str | None = None) -> tuple[str, dict[str, Any]]:
    terminal_cols = merchant_ops_repository.table_columns(engine, "terminals")
    snapshot_cols = merchant_ops_repository.table_columns(engine, "terminal_health_snapshots")
    params: dict[str, Any] = {"mid": merchant_id}
    filters: list[str] = []

    if terminal_cols:
        scope_col = "mid" if "mid" in terminal_cols else ("merchant_id" if "merchant_id" in terminal_cols else "")
        if scope_col:
            filters.append(f"t.{scope_col} = :mid")
    elif snapshot_cols:
        scope_col = "merchant_id" if "merchant_id" in snapshot_cols else ("mid" if "mid" in snapshot_cols else "")
        if scope_col:
            filters.append(f"ths.{scope_col} = :mid")

    if terminal_id:
        params["tid"] = terminal_id
        filters.append("ths.tid = :tid")

    where_sql = " AND ".join(filters) if filters else "1=1"
    return where_sql, params


def get_terminal_profile(
    engine: Any,
    *,
    merchant_id: str,
    terminal_id: str,
    source_table: str,
) -> dict[str, Any]:
    terminal_cols = merchant_ops_repository.table_columns(engine, "terminals")
    snapshot_cols = merchant_ops_repository.table_columns(engine, "terminal_health_snapshots")
    tx_cols = merchant_ops_repository.table_columns(engine, source_table)

    terminal_row: dict[str, Any] | None = None
    latest_health: dict[str, Any] | None = None
    tx_summary: dict[str, Any] = {}

    if terminal_cols and "tid" in terminal_cols:
        scope_col = "mid" if "mid" in terminal_cols else ("merchant_id" if "merchant_id" in terminal_cols else "")
        where = ["t.tid = :tid"]
        params: dict[str, Any] = {"tid": terminal_id}
        if scope_col:
            where.append(f"t.{scope_col} = :mid")
            params["mid"] = merchant_id
        query = text(
            f"""
            SELECT {", ".join(f"t.{column}" for column in sorted(terminal_cols))}
            FROM terminals t
            WHERE {' AND '.join(where)}
            LIMIT 1
            """
        )
        with engine.connect() as conn:
            row = conn.execute(query, params).mappings().first()
        if row:
            terminal_row = dict(row)
            terminal_row["terminal_id"] = str(terminal_row.get("terminal_id") or terminal_row.get("tid") or terminal_id)

    if snapshot_cols:
        where_sql, params = _scope_terminals(engine, merchant_id=merchant_id, terminal_id=terminal_id)
        query = text(
            f"""
            SELECT {", ".join(f"ths.{column}" for column in sorted(snapshot_cols))}
            FROM terminal_health_snapshots ths
            {"JOIN terminals t ON t.tid = ths.tid" if terminal_cols else ""}
            WHERE {where_sql}
            ORDER BY ths.captured_at DESC
            LIMIT 1
            """
        )
        with engine.connect() as conn:
            row = conn.execute(query, params).mappings().first()
        if row:
            latest_health = dict(row)
            latest_health["terminal_id"] = str(latest_health.get("terminal_id") or latest_health.get("tid") or terminal_id)

    if {"merchant_id", "terminal_id", "p_date", "status", "amount_rupees"}.issubset(tx_cols):
        tx_summary = merchant_ops_repository.terminal_scope_summary_from_source(
            engine,
            merchant_id,
            terminal_id,
            "1900-01-01",
            "2999-12-31",
            source_table=source_table,
        )

    evidence = [f"terminal:{terminal_id}"]
    if latest_health and str(latest_health.get("captured_at") or "").strip():
        evidence.append(f"terminal_health:{terminal_id}:{latest_health.get('captured_at')}")

    return {
        "terminal": terminal_row or {"terminal_id": terminal_id},
        "latest_health": latest_health,
        "tx_summary": tx_summary,
        "evidence": evidence,
        "error": None,
    }


def terminal_health_summary(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
    group_by: str = "tid_hour",
    limit: int = 50,
    terminal_id: str | None = None,
) -> dict[str, Any]:
    cols = merchant_ops_repository.table_columns(engine, "terminal_health_snapshots")
    if not cols:
        return {"rows": [], "evidence": [f"termhealth:{from_date}:{to_date}"], "error": "terminal_health_snapshots table not found"}

    where_sql, params = _scope_terminals(engine, merchant_id=merchant_id, terminal_id=terminal_id)
    params["d1"] = from_date
    params["d2"] = to_date

    battery_col = "battery_status" if "battery_status" in cols else ("battery_level" if "battery_level" in cols else "")
    printer_col = "printer_status" if "printer_status" in cols else ""
    network_flag_col = "low_network_strength" if "low_network_strength" in cols else ""
    quick_battery_col = "quick_battery_drainage" if "quick_battery_drainage" in cols else ""
    geo_flag_col = "latitude_longitude_deviation" if "latitude_longitude_deviation" in cols else ""
    ram_col = "ram_rom_utilization" if "ram_rom_utilization" in cols else ""

    select_parts = ["ths.tid", "ths.captured_at"]
    if network_flag_col:
        select_parts.append(f"ths.{network_flag_col} AS low_network_strength")
    if battery_col:
        select_parts.append(f"ths.{battery_col} AS battery_value")
    if quick_battery_col:
        select_parts.append(f"ths.{quick_battery_col} AS quick_battery_drainage")
    if geo_flag_col:
        select_parts.append(f"ths.{geo_flag_col} AS geo_deviation")
    if ram_col:
        select_parts.append(f"ths.{ram_col} AS ram_rom_utilization")
    if printer_col:
        select_parts.append(f"ths.{printer_col} AS printer_status")

    join_sql = "JOIN terminals t ON t.tid = ths.tid" if merchant_ops_repository.table_exists(engine, "terminals") else ""
    query = text(
        f"""
        SELECT {", ".join(select_parts)}
        FROM terminal_health_snapshots ths
        {join_sql}
        WHERE {where_sql}
          AND DATE(ths.captured_at) >= :d1
          AND DATE(ths.captured_at) < :d2
        ORDER BY ths.tid, ths.captured_at ASC
        """
    )
    with engine.connect() as conn:
        rows = [dict(row) for row in conn.execute(query, params).mappings().all()]

    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        captured_at = _parse_timestamp(row.get("captured_at"))
        hour = captured_at.hour if captured_at else None
        key = (row.get("tid"),)
        if group_by == "hour":
            key = (hour,)
        elif group_by == "tid_hour":
            key = (row.get("tid"), hour)
        grouped.setdefault(key, []).append(row)

    out: list[dict[str, Any]] = []
    for key, bucket_rows in grouped.items():
        record: dict[str, Any] = {}
        if group_by == "tid":
            record["tid"] = key[0]
        elif group_by == "hour":
            record["hour"] = key[0]
        else:
            record["tid"] = key[0]
            record["hour"] = key[1]

        record["snapshots"] = len(bucket_rows)
        net_flags = [_as_bool(row.get("low_network_strength")) for row in bucket_rows if "low_network_strength" in row]
        if net_flags:
            record["low_network_pct"] = round(100.0 * sum(1 for value in net_flags if value) / len(net_flags), 2)
        battery_values = [_as_float(row.get("battery_value")) for row in bucket_rows if "battery_value" in row]
        battery_values = [value for value in battery_values if value is not None]
        if battery_values:
            record["avg_battery_pct"] = round(sum(battery_values) / len(battery_values), 2)
        quick_flags = [_as_bool(row.get("quick_battery_drainage")) for row in bucket_rows if "quick_battery_drainage" in row]
        if quick_flags:
            record["quick_battery_drainage_pct"] = round(100.0 * sum(1 for value in quick_flags if value) / len(quick_flags), 2)
        geo_flags = [_as_bool(row.get("geo_deviation")) for row in bucket_rows if "geo_deviation" in row]
        if geo_flags:
            record["geo_deviation_pct"] = round(100.0 * sum(1 for value in geo_flags if value) / len(geo_flags), 2)
        ram_values = [_as_float(row.get("ram_rom_utilization")) for row in bucket_rows if "ram_rom_utilization" in row]
        ram_values = [value for value in ram_values if value is not None]
        if ram_values:
            record["avg_ram_rom_utilization"] = round(sum(ram_values) / len(ram_values), 2)
        printer_values = [str(row.get("printer_status")) for row in bucket_rows if row.get("printer_status") not in {None, ""}]
        if printer_values:
            record["printer_status_mode"] = Counter(printer_values).most_common(1)[0][0]
        timestamps = [ts for ts in (_parse_timestamp(row.get("captured_at")) for row in bucket_rows) if ts is not None]
        if timestamps:
            record["first_seen"] = min(timestamps)
            record["last_seen"] = max(timestamps)
        out.append(record)

    out.sort(key=lambda row: int(row.get("snapshots") or 0), reverse=True)
    evidence = [f"termhealth:{from_date}:{to_date}"]
    if terminal_id:
        evidence.append(f"tid:{terminal_id}")
    evidence.extend([f"tid:{row.get('tid')}" for row in out if row.get("tid")])
    return {
        "rows": out[: max(1, min(int(limit or 50), 200))],
        "evidence": evidence[:80],
        "window": {"from": from_date, "to": to_date},
        "group_by": group_by,
        "scope": {"merchant_id": merchant_id, "terminal_id": terminal_id},
    }


def geo_drift_check(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
    terminal_id: str | None = None,
) -> dict[str, Any]:
    cols = merchant_ops_repository.table_columns(engine, "terminal_health_snapshots")
    if "latitude" not in cols or "longitude" not in cols:
        return {"rows": [], "evidence": [f"geodrift:{from_date}:{to_date}"], "error": "latitude/longitude not present"}

    where_sql, params = _scope_terminals(engine, merchant_id=merchant_id, terminal_id=terminal_id)
    params["d1"] = from_date
    params["d2"] = to_date

    deviation_expr = "ths.latitude_longitude_deviation AS deviation_flag" if "latitude_longitude_deviation" in cols else "NULL AS deviation_flag"
    join_sql = "JOIN terminals t ON t.tid = ths.tid" if merchant_ops_repository.table_exists(engine, "terminals") else ""
    query = text(
        f"""
        SELECT
          ths.tid,
          ths.captured_at,
          ths.latitude AS lat,
          ths.longitude AS lon,
          {deviation_expr}
        FROM terminal_health_snapshots ths
        {join_sql}
        WHERE {where_sql}
          AND DATE(ths.captured_at) >= :d1
          AND DATE(ths.captured_at) < :d2
        ORDER BY ths.tid, ths.captured_at ASC
        """
    )
    with engine.connect() as conn:
        points = [dict(row) for row in conn.execute(query, params).mappings().all()]

    def haversine_distance(a: tuple[float, float], b: tuple[float, float]) -> float:
        radius = 6371000.0
        lat1, lon1 = math.radians(a[0]), math.radians(a[1])
        lat2, lon2 = math.radians(b[0]), math.radians(b[1])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        spread = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        return 2 * radius * math.asin(math.sqrt(spread))

    by_tid: dict[str, list[dict[str, Any]]] = {}
    for point in points:
        by_tid.setdefault(str(point["tid"]), []).append(point)

    rows: list[dict[str, Any]] = []
    for tid, tid_points in by_tid.items():
        coords = []
        for point in tid_points:
            lat = _as_float(point.get("lat"))
            lon = _as_float(point.get("lon"))
            if lat is not None and lon is not None:
                coords.append((lat, lon))
        max_distance_m = 0.0
        if coords:
            anchor = coords[0]
            max_distance_m = max(haversine_distance(anchor, coord) for coord in coords)
        flags = [_as_bool(point.get("deviation_flag")) for point in tid_points if point.get("deviation_flag") is not None]
        deviation_flag_pct = None
        if flags:
            deviation_flag_pct = round(100.0 * sum(1 for value in flags if value) / len(flags), 2)
        rows.append(
            {
                "tid": tid,
                "points": len(coords),
                "max_distance_m": round(max_distance_m, 1),
                "deviation_flag_pct": deviation_flag_pct,
                "first_seen": tid_points[0]["captured_at"] if tid_points else None,
                "last_seen": tid_points[-1]["captured_at"] if tid_points else None,
            }
        )

    rows.sort(key=lambda row: float(row.get("max_distance_m") or 0), reverse=True)
    evidence = [f"geodrift:{from_date}:{to_date}"]
    if terminal_id:
        evidence.append(f"tid:{terminal_id}")
    evidence.extend([f"tid:{row.get('tid')}" for row in rows if row.get("tid")])
    return {"rows": rows[:50], "evidence": evidence[:80], "window": {"from": from_date, "to": to_date}}


def terminal_issue_correlator(
    engine: Any,
    *,
    merchant_id: str,
    from_date: str,
    to_date: str,
    flag: str = "low_network_strength",
    limit: int = 20,
    terminal_id: str | None = None,
    source_table: str,
) -> dict[str, Any]:
    health_cols = merchant_ops_repository.table_columns(engine, "terminal_health_snapshots")
    if not health_cols:
        return {"rows": [], "evidence": [f"termcorr:{from_date}:{to_date}"], "error": "terminal_health_snapshots table not found"}

    flag_col = {
        "low_network_strength": "low_network_strength",
        "quick_battery_drainage": "quick_battery_drainage",
        "geo_deviation": "latitude_longitude_deviation",
    }.get(flag, "low_network_strength")
    if flag_col not in health_cols:
        return {"rows": [], "evidence": [f"termcorr:{from_date}:{to_date}"], "error": f"flag column not present: {flag_col}"}

    tx_where = [
        "merchant_id = :mid",
        "p_date >= :d1",
        "p_date < :d2",
        "terminal_id IS NOT NULL",
    ]
    tx_params: dict[str, Any] = {"mid": merchant_id, "d1": from_date, "d2": to_date}
    if terminal_id:
        tx_where.append("terminal_id = :tid")
        tx_params["tid"] = terminal_id

    tx_query = text(
        f"""
        SELECT
          terminal_id AS tid,
          CAST(COUNT(*) AS INTEGER) AS attempts,
          CAST(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS INTEGER) AS success_txns,
          CAST(SUM(CASE WHEN status IN ('FAILED', 'FAILURE') THEN 1 ELSE 0 END) AS INTEGER) AS fail_txns,
          ROUND(100.0 * SUM(CASE WHEN status IN ('FAILED', 'FAILURE') THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS fail_rate_pct
        FROM {source_table}
        WHERE {' AND '.join(tx_where)}
        GROUP BY terminal_id
        """
    )
    with engine.connect() as conn:
        tx_rows = [dict(row) for row in conn.execute(tx_query, tx_params).mappings().all()]

    where_sql, params = _scope_terminals(engine, merchant_id=merchant_id, terminal_id=terminal_id)
    params["d1"] = from_date
    params["d2"] = to_date
    join_sql = "JOIN terminals t ON t.tid = ths.tid" if merchant_ops_repository.table_exists(engine, "terminals") else ""
    health_query = text(
        f"""
        SELECT ths.tid, ths.{flag_col} AS flag_value
        FROM terminal_health_snapshots ths
        {join_sql}
        WHERE {where_sql}
          AND DATE(ths.captured_at) >= :d1
          AND DATE(ths.captured_at) < :d2
        """
    )
    with engine.connect() as conn:
        health_rows = [dict(row) for row in conn.execute(health_query, params).mappings().all()]

    by_tid: dict[str, list[bool]] = {}
    for row in health_rows:
        flag_value = _as_bool(row.get("flag_value"))
        if flag_value is None:
            continue
        by_tid.setdefault(str(row.get("tid")), []).append(flag_value)

    merged: list[dict[str, Any]] = []
    for tx_row in tx_rows:
        tid = str(tx_row.get("tid") or "")
        flags = by_tid.get(tid) or []
        if not flags:
            continue
        flag_pct = round(100.0 * sum(1 for value in flags if value) / len(flags), 2)
        if flag_pct <= 0:
            continue
        merged.append(
            {
                "tid": tid,
                "attempts": int(tx_row.get("attempts") or 0),
                "fail_txns": int(tx_row.get("fail_txns") or 0),
                "fail_rate_pct": float(tx_row.get("fail_rate_pct") or 0.0),
                "flag_pct": flag_pct,
                "snapshots": len(flags),
            }
        )

    merged.sort(key=lambda row: float(row.get("fail_rate_pct") or 0.0) * float(row.get("flag_pct") or 0.0), reverse=True)
    evidence = [f"termcorr:{flag}:{from_date}:{to_date}"]
    if terminal_id:
        evidence.append(f"tid:{terminal_id}")
    evidence.extend([f"tid:{row.get('tid')}" for row in merged if row.get("tid")])
    return {
        "rows": merged[: max(1, min(int(limit or 20), 50))],
        "evidence": evidence[:80],
        "window": {"from": from_date, "to": to_date},
        "flag": flag,
        "scope": {"merchant_id": merchant_id, "terminal_id": terminal_id},
    }
