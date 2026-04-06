from sqlalchemy import create_engine, inspect
from config import Config

engine = create_engine(Config.DATABASE_URL)
inspector = inspect(engine)
print("Merchants columns:", [c['name'] for c in inspector.get_columns('merchants')])
