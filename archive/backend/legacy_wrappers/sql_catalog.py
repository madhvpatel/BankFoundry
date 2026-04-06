from app.legacy.copilot import sql_catalog as _legacy_sql_catalog
import sys
sys.modules[__name__] = _legacy_sql_catalog
