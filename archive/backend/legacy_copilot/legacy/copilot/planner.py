from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_ollama import ChatOllama

from config import Config
from .md import load_agent_docs

logger = logging.getLogger("copilot_planner")


@dataclass
class Plan:
    intent: str
    tool_calls: list[dict[str, Any]]


_PLAN_SYSTEM_FALLBACK = """You are a merchant-facing payments copilot planner.
You decide which tools to call to answer the merchant question.

Rules:
- Always stay scoped to the provided merchant_id.
- Prefer 1-3 tool calls.
- If the user message is a greeting/smalltalk (e.g. hi/hello/thanks), choose intent=general and tool_calls=[].
- If the question is general knowledge (no merchant data), you may answer without tools.
- Return ONLY JSON (no prose, no <think>).

JSON schema:
{
  "intent": "kyc|transactions|settlements|chargebacks|refunds|general",
  "tool_calls": [
    {"name": "tool_name", "args": {"...": "..."}}
  ]
}
"""


def _extract_json_object(raw: str) -> dict[str, Any] | None:
    text = (raw or "").strip()
    if not text:
        return None
    text = re.sub(r"^```(json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass

    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        return None
    try:
        parsed = json.loads(m.group(0))
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def build_plan(question: str, merchant_id: str, agent_dir: Path) -> Plan:
    docs = load_agent_docs(agent_dir)

    # Force deterministic, machine-parseable output.
    # Ollama supports `format: "json"` on /api/chat; some langchain_ollama versions
    # accept this as `format=...` (others may not), so we keep a safe fallback.
    try:
        llm = ChatOllama(
            model=Config.OLLAMA_MODEL,
            base_url=Config.OLLAMA_BASE_URL,
            temperature=0.0,
            format="json",
        )
    except TypeError:
        llm = ChatOllama(
            model=Config.OLLAMA_MODEL,
            base_url=Config.OLLAMA_BASE_URL,
            temperature=0.0,
        )

    system = (docs.root + "\n\n" + docs.tools + "\n\n" + docs.rubrics).strip()
    # Always include a strict schema block, even if docs are present.
    # This reduces the chance the planner emits prose.
    if system:
        system = system + "\n\n" + _PLAN_SYSTEM_FALLBACK
    else:
        system = _PLAN_SYSTEM_FALLBACK

    user = {
        "merchant_id": merchant_id,
        "question": question,
        "today": "(use relative ranges if needed; otherwise ask tools for explicit windows)",
        "tool_args_conventions": {
            "from_date": "YYYY-MM-DD",
            "to_date": "YYYY-MM-DD",
            "limit": "<= 200",
        },
    }

    try:
        resp = llm.invoke(
            [
                SystemMessage(content=system + "\n\nReturn ONLY JSON."),
                HumanMessage(content=json.dumps(user, ensure_ascii=False, indent=2)),
            ]
        )
        raw = resp.content if hasattr(resp, "content") else str(resp)
    except Exception as exc:
        logger.warning("Planner LLM failed: %s", exc)
        return Plan(intent="general", tool_calls=[])

    obj = _extract_json_object(raw)
    if not obj:
        # One repair attempt: ask the model to convert its own output into the required JSON.
        try:
            repair = llm.invoke(
                [
                    SystemMessage(
                        content=(
                            _PLAN_SYSTEM_FALLBACK
                            + "\n\nConvert the following text into a SINGLE JSON object that matches the schema."
                            + " Output JSON only."
                        )
                    ),
                    HumanMessage(content=raw),
                ]
            )
            raw2 = repair.content if hasattr(repair, "content") else str(repair)
            obj = _extract_json_object(raw2)
        except Exception:
            obj = None

    if not obj:
        logger.info("Planner produced non-JSON; falling back to general")
        return Plan(intent="general", tool_calls=[])

    intent = str(obj.get("intent") or "general").strip().lower()
    if intent not in {"kyc", "transactions", "settlements", "chargebacks", "refunds", "general"}:
        intent = "general"

    tool_calls_raw = obj.get("tool_calls")
    tool_calls: list[dict[str, Any]] = []
    if isinstance(tool_calls_raw, list):
        for c in tool_calls_raw[:4]:
            if not isinstance(c, dict):
                continue
            name = str(c.get("name") or "").strip()
            args = c.get("args")
            if not name or not isinstance(args, dict):
                continue
            tool_calls.append({"name": name, "args": args})

    return Plan(intent=intent, tool_calls=tool_calls)
