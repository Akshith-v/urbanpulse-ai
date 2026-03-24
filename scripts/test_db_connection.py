from sqlalchemy import text
from backend.database.session import engine

with engine.connect() as conn:
    result = conn.execute(text("SELECT current_database();"))
    print("Connected to:", result.scalar())
    