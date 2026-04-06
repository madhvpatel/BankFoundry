from app.legacy.copilot import planner as _legacy_planner
import sys
sys.modules[__name__] = _legacy_planner
