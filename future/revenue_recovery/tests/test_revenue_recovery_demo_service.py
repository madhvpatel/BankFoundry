from decimal import Decimal
from unittest.mock import patch

from app.revenue_recovery.demo_service import run_preview_turn
from app.revenue_recovery.models import ActionLevel, UserRole


def test_run_preview_turn_returns_clarification_for_vague_prompt():
    payload = run_preview_turn(
        None,
        merchant_id="merchant_001",
        prompt="help",
        user_role=UserRole.ops,
        requested_action_level=ActionLevel.read_only,
    )

    assert payload["clarification_request"] is not None
    assert payload["response"]["executive_summary"] == payload["clarification_request"]["question"]
    assert payload["status"] == "completed"


@patch("app.revenue_recovery.demo_service._execute_compiled_query")
def test_run_preview_turn_returns_completed_preview_response(mock_execute):
    mock_execute.side_effect = [
        [
            {
                "total_attempts": 120,
                "failed_attempts": 30,
                "failed_gmv": Decimal("580000"),
                "success_rate": Decimal("0.75"),
            }
        ],
        [
            {
                "total_attempts": 120,
                "failed_attempts": 20,
                "failed_gmv": Decimal("300000"),
                "success_rate": Decimal("0.83"),
            }
        ],
        [
            {"response_code": "91", "failed_attempts": 12, "failed_gmv": Decimal("220000")},
            {"response_code": "05", "failed_attempts": 7, "failed_gmv": Decimal("150000")},
        ],
        [
            {"terminal_id": "TERM_001", "failed_attempts": 8, "failed_gmv": Decimal("180000")},
        ],
        [
            {"payment_mode": "CARD", "failed_attempts": 30, "failed_gmv": Decimal("580000")},
        ],
    ]

    payload = run_preview_turn(
        object(),
        merchant_id="merchant_001",
        prompt="Why did failures increase in the last 30 days?",
        user_role=UserRole.ops,
        requested_action_level=ActionLevel.read_only,
    )

    assert payload["status"] == "completed"
    assert payload["coverage_score"] >= 0.8
    assert payload["response"]["findings"]
    assert payload["state"]["diagnosis"]["ranked_drivers"][0]["driver_type"] == "response_code_concentration"
    assert any(trace["node_name"] == "compose_response" for trace in payload["traces"])
