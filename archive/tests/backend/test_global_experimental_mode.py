import unittest
from pathlib import Path
from unittest.mock import patch

from langchain_core.tools import tool
from sqlalchemy import create_engine, text

from config import Config
from app.copilot.runtime import run_turn
from app.copilot.toolcalling import invoke_with_tools, make_tools
from app.copilot.tools import ToolContext, sql_database


class GlobalToolCatalogTest(unittest.TestCase):
    def test_make_tools_includes_global_aliases(self):
        ctx = ToolContext(engine=object(), merchant_id="merchant_001")
        names = {t.name for t in make_tools(ctx=ctx, default_from="2026-01-01", default_to="2026-01-31")}
        self.assertIn("sql_database", names)
        self.assertIn("verify_failure_drivers", names)
        self.assertIn("knowledge_base", names)
        self.assertIn("merchant_profile", names)
        self.assertIn("startup_kpis", names)


class SqlDatabaseGuardrailsTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                """
                CREATE TABLE transaction_features (
                    merchant_id TEXT,
                    amount_rupees REAL
                )
                """
            )
            conn.execute(
                text(
                    """
                    INSERT INTO transaction_features (merchant_id, amount_rupees) VALUES
                    ('merchant_001', 100.0),
                    ('merchant_002', 200.0)
                    """
                )
            )
        self.ctx = ToolContext(engine=self.engine, merchant_id="merchant_001")

    def test_sql_database_enforces_mid_placeholder(self):
        out = sql_database(self.ctx, query="SELECT * FROM transaction_features", limit=10)
        self.assertIn("error", out)
        self.assertIn(":mid", out["error"])
        self.assertFalse(out.get("verified"))
        self.assertEqual(out.get("error_code"), "scope_violation")

    def test_sql_database_blocks_write_queries(self):
        out = sql_database(
            self.ctx,
            query="DELETE FROM transaction_features WHERE merchant_id = :mid",
            limit=10,
        )
        self.assertIn("error", out)
        self.assertIn("read-only", out["error"])
        self.assertFalse(out.get("verified"))

    def test_sql_database_returns_scoped_rows(self):
        out = sql_database(
            self.ctx,
            query=(
                "SELECT merchant_id, amount_rupees "
                "FROM transaction_features "
                "WHERE merchant_id = :mid "
                "ORDER BY amount_rupees DESC"
            ),
            limit=10,
        )
        self.assertTrue(out.get("verified"))
        self.assertEqual(out.get("row_count"), 1)
        self.assertEqual(out.get("rows", [])[0]["merchant_id"], "merchant_001")

    def test_sql_database_returns_structured_table_error(self):
        out = sql_database(
            self.ctx,
            query="SELECT * FROM transactions WHERE merchant_id = :mid",
            limit=10,
        )
        self.assertFalse(out.get("verified"))
        self.assertEqual(out.get("error_code"), "undefined_table")
        self.assertTrue(isinstance(out.get("schema_hint"), dict))


class InvokeWithToolsLoopTest(unittest.TestCase):
    class _FakeAI:
        def __init__(self, content, tool_calls):
            self.content = content
            self.tool_calls = tool_calls

    class _FakeLLM:
        def __init__(self, *args, **kwargs):
            self._calls = 0

        def bind_tools(self, tools):
            self._tools = tools
            return self

        def invoke(self, messages):
            self._calls += 1
            if self._calls == 1:
                return InvokeWithToolsLoopTest._FakeAI(
                    "",
                    [{"id": "call_1", "name": "echo_tool", "args": {"value": 7}}],
                )
            return InvokeWithToolsLoopTest._FakeAI("Final model answer.", [])

    def test_invoke_with_tools_supports_multi_step(self):
        @tool("echo_tool")
        def echo_tool(value: int) -> dict:
            """Echo tool for loop test."""
            return {"value": value}

        with patch("app.copilot.toolcalling.ChatOllama", self._FakeLLM):
            tool_calls, results, final_text = invoke_with_tools(
                system="test-system",
                user={"q": "test"},
                tools=[echo_tool],
                temperature=0.1,
                max_steps=3,
            )

        self.assertEqual(len(tool_calls), 1)
        self.assertEqual(tool_calls[0]["name"], "echo_tool")
        self.assertEqual(len(results), 1)
        self.assertTrue(results[0]["ok"])
        self.assertEqual(results[0]["output"], {"value": 7})
        self.assertEqual(final_text, "Final model answer.")


class GlobalRuntimeModeTest(unittest.TestCase):
    def test_run_turn_uses_single_agent_final_text_when_available(self):
        with patch.object(Config, "GLOBAL_EXPERIMENTAL_MODE", True), patch.object(
            Config, "GLOBAL_EXPERIMENTAL_MAX_STEPS", 5
        ), patch(
            "app.copilot.runtime.default_window_from_max_date",
            return_value=("2026-01-01", "2026-02-01"),
        ), patch(
            "app.copilot.runtime.make_tools",
            return_value=[],
        ), patch(
            "app.copilot.runtime.invoke_with_tools",
            return_value=(
                [{"name": "compute_kpis", "args": {"from_date": "2026-01-01", "to_date": "2026-02-01"}}],
                [{"tool": "compute_kpis", "ok": True, "output": {"evidence": ["kpi:demo"]}, "error": None}],
                "Direct answer from tool-calling model.",
            ),
        ) as mock_invoke, patch(
            "app.copilot.runtime._render_answer",
            side_effect=AssertionError("narrator should not run in this path"),
        ):
            turn = run_turn(
                engine=object(),
                agent_dir=Path("/tmp"),
                merchant_id="merchant_001",
                question="How did performance look last month?",
            )

        self.assertIn("Direct answer from tool-calling model.", turn.answer)
        self.assertIn("Evidence IDs: kpi:demo", turn.answer)
        self.assertEqual(turn.evidence, ["kpi:demo"])
        budgets = [call.kwargs["max_steps"] for call in mock_invoke.call_args_list]
        self.assertTrue(budgets)
        self.assertLessEqual(max(budgets), 2)


if __name__ == "__main__":
    unittest.main()
