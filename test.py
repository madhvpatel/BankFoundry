from app.intelligence.runner import run_intelligence
from config import Config
from sqlalchemy import create_engine


config = Config()
engine = create_engine(config.DATABASE_URL)

payload = run_intelligence(engine, "merchant_123")
print(payload.get("recos", []))
