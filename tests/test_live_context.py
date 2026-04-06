import unittest
from unittest.mock import Mock

from app.application.workflows.live_context import (
    LiveContextDeps,
    build_merchant_snapshot,
    build_report_payload,
    merchant_label,
)


class LiveContextTest(unittest.TestCase):
    def test_build_merchant_snapshot_adds_scope_and_refresh_metadata(self):
        engine = object()
        deps = LiveContextDeps(
            engine=engine,
            json_safe=lambda value: value,
            ensure_background_proactive_refresh=Mock(return_value={"due": False}),
            get_merchant_os_snapshot=Mock(return_value={"merchant_id": "merchant_001", "proactive_cards": []}),
            terminal_scope_options=Mock(return_value=["T1", "T2"]),
            scope_snapshot_to_terminal=Mock(return_value={"merchant_id": "merchant_001", "scope": {"level": "terminal"}}),
            get_background_refresh_status=Mock(return_value={"due": False, "next_refresh_at": "2026-03-10T10:00:00+00:00"}),
            build_report_briefs=Mock(return_value=[]),
            build_report_packs=Mock(return_value=[]),
        )

        snapshot = build_merchant_snapshot(
            "merchant_001",
            "T1",
            days=30,
            refresh=True,
            deps=deps,
        )

        self.assertEqual(snapshot["selected_terminal_id"], "T1")
        self.assertEqual(snapshot["terminal_options"], ["T1", "T2"])
        self.assertEqual(snapshot["refresh_status"]["due"], False)
        deps.ensure_background_proactive_refresh.assert_called_once_with(engine, "merchant_001", days=30, force=False)
        deps.scope_snapshot_to_terminal.assert_called_once_with(engine, {"merchant_id": "merchant_001", "proactive_cards": []}, "T1")

    def test_report_payload_and_label_are_sanitized(self):
        deps = LiveContextDeps(
            engine=object(),
            json_safe=lambda value: value,
            ensure_background_proactive_refresh=Mock(),
            get_merchant_os_snapshot=Mock(),
            terminal_scope_options=Mock(),
            scope_snapshot_to_terminal=Mock(),
            get_background_refresh_status=Mock(),
            build_report_briefs=Mock(
                return_value=[
                    {
                        "id": "brief_1",
                        "title": "Daily Brief",
                        "subject": "Subject",
                        "summary_lines": ["line 1"],
                        "dataset_lines": ["dataset 1"],
                        "email_text": "Email body",
                        "print_html": "<p>body</p>",
                    }
                ]
            ),
            build_report_packs=Mock(
                return_value=[
                    {
                        "id": "pack_1",
                        "title": "Pack",
                        "summary_lines": ["summary"],
                        "datasets": [{"name": "main"}],
                    }
                ]
            ),
        )

        payload = build_report_payload({"merchant_profile": {"merchant": {"merchant_trade_name": "Demo Store"}}}, deps=deps)

        self.assertEqual(payload["briefs"][0]["id"], "brief_1")
        self.assertEqual(payload["packs"][0]["id"], "pack_1")
        self.assertEqual(
            merchant_label({"merchant_profile": {"merchant": {"merchant_trade_name": "Demo Store"}}}, "merchant_001"),
            "Demo Store",
        )


if __name__ == "__main__":
    unittest.main()
