import sys
from sqlalchemy import create_engine
from app.config import Config
from app.copilot.tools import get_merchant_lending_offers
from app.copilot.toolcalling import ToolContext

engine = create_engine(Config.DATABASE_URL)
ctx = ToolContext(engine=engine, merchant_id="100000000121215", db_url=Config.DATABASE_URL)
result = get_merchant_lending_offers(ctx)
print(result)
