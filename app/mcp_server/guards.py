from __future__ import annotations

from datetime import date


class MCPGuardError(ValueError):
    pass


def require_merchant_id(merchant_id: str) -> str:
    value = str(merchant_id or "").strip()
    if not value:
        raise MCPGuardError("merchant_id is required")
    return value


def bounded_window(start_date: date, end_date: date, *, max_days: int = 180) -> tuple[date, date]:
    if end_date <= start_date:
        raise MCPGuardError("end_date must be after start_date")
    if (end_date - start_date).days > max_days:
        raise MCPGuardError(f"date window exceeds {max_days} days")
    return start_date, end_date


def bounded_limit(limit: int, *, minimum: int = 1, maximum: int = 20) -> int:
    value = int(limit)
    if value < minimum or value > maximum:
        raise MCPGuardError(f"limit must be between {minimum} and {maximum}")
    return value
