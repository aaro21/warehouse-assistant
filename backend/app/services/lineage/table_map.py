# backend/app/services/lineage/table_map.py
from fastapi import APIRouter
from sqlalchemy import text
from backend.app.core.database import engine

router = APIRouter()

@router.get("/lineage/tables")
def get_lineage_tables():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM aud.vw_recursive_table_lineage"))
        rows = [dict(row) for row in result]
        return rows