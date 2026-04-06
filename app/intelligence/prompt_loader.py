from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger("prompt_loader")


def _extract_section(markdown: str, section: str) -> str:
    target = f"## {section}".strip().lower()
    lines = markdown.splitlines()
    collecting = False
    buf: list[str] = []

    for line in lines:
        if line.startswith("## "):
            current = line.strip().lower()
            if collecting:
                break
            collecting = current == target
            continue
        if collecting:
            buf.append(line)

    return "\n".join(buf).strip()


def load_prompt_section(agents_md_path: Path, section: str, fallback: str) -> str:
    """
    Load prompt content from a named section in an AGENTS.md file.
    Falls back to the provided in-code prompt if file/section is unavailable.
    """
    try:
        text = agents_md_path.read_text(encoding="utf-8")
    except Exception as exc:
        logger.debug("AGENTS.md not readable at %s: %s", agents_md_path, exc)
        return fallback

    section_text = _extract_section(text, section)
    if section_text:
        return section_text

    logger.debug("Section '%s' not found in %s", section, agents_md_path)
    return fallback
