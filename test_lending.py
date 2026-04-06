import asyncio
from sqlalchemy import create_engine
from app.growth.lending_engine import get_lending_offers
from config import Config

engine = create_engine(Config.DATABASE_URL)

try:
    print(get_lending_offers(engine, "100000000121215"))
except Exception as e:
    print("Error:", e)
