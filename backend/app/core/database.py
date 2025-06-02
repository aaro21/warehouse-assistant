# backend/app/core/database.py

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import URL
import os

# You can set this in your .env file and load it with os.getenv
DATABASE_URL = URL.create(
    "mssql+pyodbc",
    username=os.getenv("DB_SQL_USER", "sa"),
    password=os.getenv("DB_SQL_PASSWORD", ""),
    host=os.getenv("DB_SQL_SERVER", "localhost"),
    database=os.getenv("DB_SQL_DB", "ai_assistant"),
    query={
        "driver": "ODBC Driver 18 for SQL Server",
        "TrustServerCertificate": "yes",
    },
)
# Create engine and session factory
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for models
Base = declarative_base()

# Dependency to be used in FastAPI routes
def get_db():
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()