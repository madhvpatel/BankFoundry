from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, is_dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text

from config import Config
from .runtime import run_turn

logger = logging.getLogger("copilot_validation_server")
AGENT_DIR = Path(__file__).resolve().parents[3] / "agent"


class ValidationRequestError(ValueError):
    pass


def _json_safe(value: Any) -> Any:
    if is_dataclass(value):
        value = asdict(value)
    return json.loads(json.dumps(value, default=str, ensure_ascii=False))


def pick_default_merchant_id(engine: Any) -> str:
    table = str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features"))
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT merchant_id FROM {table} LIMIT 1")).fetchone()
    return str(row[0]).strip() if row and row[0] is not None else ""


def pick_default_terminal_id(engine: Any, merchant_id: str) -> str | None:
    if not str(merchant_id or "").strip():
        return None
    table = str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features"))
    with engine.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT terminal_id
                FROM {table}
                WHERE merchant_id = :mid
                  AND terminal_id IS NOT NULL
                  AND TRIM(CAST(terminal_id AS TEXT)) <> ''
                ORDER BY terminal_id
                LIMIT 1
                """
            ),
            {"mid": merchant_id},
        ).fetchone()
    return str(row[0]).strip() if row and row[0] is not None else None


def build_test_ask_response(engine: Any, payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValidationRequestError("request body must be a JSON object")

    prompt = str(payload.get("prompt") or "").strip()
    if not prompt:
        raise ValidationRequestError("prompt is required")

    lane = str(payload.get("lane") or "").strip().lower()
    if lane not in {"operations", "growth"}:
        raise ValidationRequestError("lane must be one of: operations, growth")

    merchant_id = str(payload.get("merchant_id") or "").strip()
    if not merchant_id:
        merchant_id = pick_default_merchant_id(engine)
    if not merchant_id:
        raise ValidationRequestError("merchant_id is required or must be discoverable from the configured source table")

    terminal_id = str(payload.get("terminal_id") or "").strip() or None
    if lane == "growth" and terminal_id is None:
        requested_terminal = payload.get("auto_terminal")
        if requested_terminal is True:
            terminal_id = pick_default_terminal_id(engine, merchant_id)

    turn = run_turn(
        engine=engine,
        agent_dir=AGENT_DIR,
        merchant_id=merchant_id,
        question=prompt,
        forced_lane=lane,
        terminal_id=terminal_id,
    )

    return {
        "merchant_id": merchant_id,
        "prompt": prompt,
        "lane": lane,
        "terminal_id": terminal_id,
        "answer": turn.answer,
        "operations_section": _json_safe(turn.operations_section),
        "growth_section": _json_safe(turn.growth_section),
        "tool_calls": _json_safe(turn.tool_calls),
        "tool_results": _json_safe(turn.tool_results),
        "evidence": _json_safe(turn.evidence),
        "terminal_focus": turn.terminal_focus,
        "intent": turn.intent,
        "active_lane": turn.active_lane,
    }


class ValidationHTTPServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], engine: Any):
        super().__init__(server_address, ValidationRequestHandler)
        self.engine = engine


class ValidationRequestHandler(BaseHTTPRequestHandler):
    server: ValidationHTTPServer

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        logger.info("validation-server %s - %s", self.address_string(), format % args)

    def _send_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/test/ask":
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return

        try:
            length = int(self.headers.get("Content-Length") or "0")
        except ValueError:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid_content_length"})
            return

        try:
            raw = self.rfile.read(length) if length > 0 else b"{}"
            payload = json.loads(raw.decode("utf-8")) if raw else {}
        except Exception:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid_json"})
            return

        try:
            response = build_test_ask_response(self.server.engine, payload)
        except ValidationRequestError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return
        except Exception as exc:  # pragma: no cover - defensive guard for manual debugging
            logger.exception("validation endpoint failed")
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})
            return

        self._send_json(HTTPStatus.OK, response)


def create_validation_server(host: str = "127.0.0.1", port: int = 8765, *, engine: Any | None = None) -> ValidationHTTPServer:
    db_engine = engine or create_engine(Config.DATABASE_URL)
    return ValidationHTTPServer((host, port), db_engine)


def main() -> None:
    parser = argparse.ArgumentParser(description="Local-only test wrapper for run_turn")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8765, help="Bind port (default: 8765)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    server = create_validation_server(args.host, args.port)
    logger.info("Validation server listening on http://%s:%s/test/ask", args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Validation server shutting down")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
