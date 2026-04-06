from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from .models import InvestigationState, PersistedTraceManifest, QuerySpec


class InMemoryCheckpointStore:
    def __init__(self) -> None:
        self._states: dict[str, dict[str, Any]] = {}
        self._traces: dict[str, dict[str, Any]] = {}
        self._query_specs: dict[tuple[str, str], list[dict[str, Any]]] = {}

    @staticmethod
    def _ref(prefix: str, payload: str) -> str:
        return f"{prefix}_{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:16]}"

    def persist_state(self, run_id: str, node_name: str, state: InvestigationState) -> str:
        payload = state.model_dump_json()
        ref = self._ref("state", f"{run_id}:{node_name}:{payload}")
        self._states[ref] = state.model_dump(mode="python")
        return ref

    def load_state(self, ref: str) -> InvestigationState:
        return InvestigationState.model_validate(self._states[ref])

    def persist_trace(
        self,
        *,
        run_id: str,
        node_name: str,
        input_state_ref: str,
        output_state_ref: str,
        context_manifest_version: str,
        prompt_template_version: str | None = None,
        tool_call_refs: list[str] | None = None,
        llm_call_ref: str | None = None,
    ) -> PersistedTraceManifest:
        created_at = datetime.now(timezone.utc)
        manifest = PersistedTraceManifest(
            run_id=run_id,
            node_name=node_name,
            checkpoint_ref=output_state_ref,
            input_state_ref=input_state_ref,
            output_state_ref=output_state_ref,
            context_manifest_version=context_manifest_version,
            prompt_template_version=prompt_template_version,
            tool_call_refs=list(tool_call_refs or []),
            llm_call_ref=llm_call_ref,
            created_at=created_at,
            started_at=created_at,
            finished_at=created_at,
        )
        ref = self._ref("trace", manifest.model_dump_json())
        self._traces[ref] = manifest.model_dump(mode="python")
        return manifest

    def load_trace(self, ref: str) -> PersistedTraceManifest:
        return PersistedTraceManifest.model_validate(self._traces[ref])

    def list_traces(self, run_id: str) -> list[PersistedTraceManifest]:
        traces = []
        for payload in self._traces.values():
            if payload.get("run_id") == run_id:
                traces.append(PersistedTraceManifest.model_validate(payload))
        traces.sort(key=lambda item: item.created_at)
        return traces

    def persist_query_specs(self, run_id: str, node_name: str, query_specs: list[QuerySpec]) -> None:
        self._query_specs[(run_id, node_name)] = [item.model_dump(mode="python") for item in query_specs]

    def load_query_specs(self, run_id: str, node_name: str) -> list[QuerySpec]:
        items = self._query_specs.get((run_id, node_name), [])
        return [QuerySpec.model_validate(item) for item in items]
