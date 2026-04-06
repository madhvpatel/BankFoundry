from app.legacy.copilot import runtime as _legacy_runtime
import sys
sys.modules[__name__] = _legacy_runtime
