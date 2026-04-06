from functools import lru_cache

from sqlalchemy import create_engine, text

from config import Config

from .source_adapters import resolve_transaction_source


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def scale_inr(value, amount_scale: float) -> float:
    return _safe_float(value) * float(amount_scale or 1.0)


@lru_cache(maxsize=16)
def _cached_scale_for_url(db_url: str) -> float:
    mode = (Config.DB_AMOUNT_SCALE_MODE or "auto").lower()
    fixed = float(Config.DB_AMOUNT_SCALE_FACTOR or 1.0)

    if mode == "fixed":
        return fixed

    engine = create_engine(db_url)
    provider = resolve_transaction_source(engine)
    if provider.missing("status", "amount_rupees"):
        return fixed

    try:
        if provider.has("amount_paise"):
            ratio_sql = text(
                f"""
                SELECT
                  AVG({provider.value('amount_paise')}) / 100.0 AS avg_rupees_from_paise,
                  AVG({provider.value('amount_rupees')}) AS avg_rupees
                FROM {provider.source_table}
                WHERE {provider.value('status')} = 'SUCCESS'
                  AND {provider.value('amount_paise')} IS NOT NULL
                  AND {provider.value('amount_rupees')} IS NOT NULL
                """
            )
            with engine.connect() as conn:
                row = conn.execute(ratio_sql).fetchone()
            avg_from_paise = _safe_float(getattr(row, "avg_rupees_from_paise", None), 0.0)
            avg_rupees = _safe_float(getattr(row, "avg_rupees", None), 0.0)
            if avg_from_paise > 0 and avg_rupees > 0:
                ratio = avg_from_paise / avg_rupees
                if 0.95 <= ratio <= 1.05:
                    return 1.0
                if 9.5 <= ratio <= 10.5:
                    return 0.1
                if 95 <= ratio <= 105:
                    return 0.01
    except Exception:
        pass

    try:
        avg_sql = text(
            f"""
            SELECT AVG({provider.value('amount_rupees')})
            FROM {provider.source_table}
            WHERE {provider.value('status')} = 'SUCCESS'
            """
        )
        with engine.connect() as conn:
            avg_success_ticket = conn.execute(avg_sql).scalar()
    except Exception:
        return fixed

    avg_ticket = _safe_float(avg_success_ticket, 0.0)
    threshold = float(Config.DB_AMOUNT_AUTO_THRESHOLD or 10000.0)
    auto_factor = float(Config.DB_AMOUNT_AUTO_FACTOR or 0.1)

    if avg_ticket > threshold:
        return auto_factor
    return fixed


def get_amount_scale(engine=None) -> float:
    mode = (Config.DB_AMOUNT_SCALE_MODE or "auto").lower()
    fixed = float(Config.DB_AMOUNT_SCALE_FACTOR or 1.0)
    if mode == "fixed":
        return fixed

    if engine is not None:
        try:
            db_url = engine.url.render_as_string(hide_password=False)
            return _cached_scale_for_url(db_url)
        except Exception:
            return fixed
    return _cached_scale_for_url(Config.DATABASE_URL)
