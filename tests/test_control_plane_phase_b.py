import unittest
from unittest.mock import patch

from app.api import server
from app.application.kernel.request_models import RequestType, Surface


class ControlPlanePhaseBTest(unittest.TestCase):
    def test_chat_handler_delegates_to_workflow_with_runtime_bound_dependencies(self):
        request = server._canonical_request(
            request_type=RequestType.chat_turn,
            surface=Surface.web_chat,
            merchant_id="merchant_001",
            payload={"prompt": "Hello", "history": []},
            debug=True,
            thread_scope="default",
        )

        captured = {}

        def fake_handle(req, deps):
            captured["request"] = req
            captured["deps"] = deps
            return {"answer": "delegated"}

        with patch("app.api.server.run_agent_turn") as mock_run_agent_turn, patch(
            "app.api.server.merchant_surface.handle_chat_turn",
            side_effect=fake_handle,
        ) as mock_workflow_handler:
            payload = server._handle_chat_turn(request)

        self.assertEqual(payload["answer"], "delegated")
        self.assertEqual(captured["request"], request)
        self.assertIs(captured["deps"].run_agent_turn, mock_run_agent_turn)
        self.assertEqual(captured["deps"].engine, server.engine)
        mock_workflow_handler.assert_called_once()


if __name__ == "__main__":
    unittest.main()
