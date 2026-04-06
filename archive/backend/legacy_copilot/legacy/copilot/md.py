from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AgentDocs:
    root: str
    tone: str
    tools: str
    rubrics: str


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def load_agent_docs(agent_dir: Path) -> AgentDocs:
    """Load MD steering docs from `agent/` folder.

    We keep this dead-simple: no templating, no sections; the whole file is loaded.
    """
    return AgentDocs(
        root=_read_text(agent_dir / "ROOT.md"),
        tone=_read_text(agent_dir / "TONE.md"),
        tools=_read_text(agent_dir / "TOOLS.md"),
        rubrics=_read_text(agent_dir / "RUBRICS.md"),
    )
