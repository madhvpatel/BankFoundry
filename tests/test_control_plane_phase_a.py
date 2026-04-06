import unittest

from app.application.control_plane.router import ControlPlaneRouter
from app.application.control_plane.sessions import build_session_key
from app.application.kernel.request_models import (
    ActorContext,
    CanonicalRequest,
    DeliveryContext,
    PolicyContext,
    RequestType,
    SessionContext,
    Surface,
    TenantContext,
    WorkspaceContext,
)


class ControlPlanePhaseATest(unittest.TestCase):
    def test_build_session_key_for_chat_terminal_scope(self):
        key = build_session_key(
            request_type=RequestType.chat_turn,
            surface=Surface.web_chat,
            merchant_id="merchant_001",
            terminal_id="TERM_01",
        )
        self.assertEqual(key, "merchant:merchant_001:chat:web_chat:terminal:TERM_01")

    def test_build_session_key_for_workspace_scope(self):
        key = build_session_key(
            request_type=RequestType.workspace_refresh,
            surface=Surface.workspace,
            merchant_id="merchant_001",
        )
        self.assertEqual(key, "merchant:merchant_001:workspace")

    def test_control_plane_router_wraps_payload_in_canonical_response(self):
        request = CanonicalRequest(
            request_id="req_demo",
            request_type=RequestType.chat_turn,
            surface=Surface.web_chat,
            actor=ActorContext(actor_id="api_caller"),
            tenant=TenantContext(tenant_id="merchant_001"),
            workspace=WorkspaceContext(merchant_id="merchant_001"),
            session=SessionContext(session_key="merchant:merchant_001:chat:web_chat:default"),
            payload={"prompt": "Hello"},
            policy_context=PolicyContext(),
            delivery=DeliveryContext(),
        )

        router = ControlPlaneRouter(
            handlers={
                RequestType.chat_turn.value: lambda req: {"answer": f"echo:{req.payload['prompt']}"},
            }
        )

        response = router.handle(request)
        self.assertEqual(response.request_id, "req_demo")
        self.assertEqual(response.session_key, "merchant:merchant_001:chat:web_chat:default")
        self.assertEqual(response.payload["answer"], "echo:Hello")
        self.assertEqual(response.trace["request_type"], "chat_turn")


if __name__ == "__main__":
    unittest.main()
