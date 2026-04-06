from app.legacy.copilot import sql_graph_agent as _legacy_sql_graph_agent
import sys
sys.modules[__name__] = _legacy_sql_graph_agent
