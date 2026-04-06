from typing import Any
def _collect_sources(tool_results: list[dict[str, Any]]) -> list[str]:
    sources: list[str] = []
    for result in tool_results:
        output = result.get("output")
        if not isinstance(output, dict):
            continue
        for item in list(output.get("evidence") or [])[:80]:
            source = str(item).strip()
            if source and source not in sources:
                sources.append(source)
    return sources

print(_collect_sources([{"output": {"evidence": ["lending_assessment:123"]}}]))
