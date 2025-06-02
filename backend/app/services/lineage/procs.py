from fastapi import APIRouter
from sqlmodel import Session, select
from app.services.lineage.models_sql import ProcMetadata
from app.core.database import engine

router = APIRouter()

def run_full_lineage_population(db):
    # Placeholder logic for populating lineage-related tables
    # You will replace these with actual function calls later
    results = {
        "stage_to_bronze": "TODO: stage to bronze mapping logic",
        "bronze_to_silver": "TODO: bronze to silver stored procedure parsing",
        "silver_to_gold": "TODO: silver to gold stored procedure parsing"
    }

@router.get("/api/lineage/procs")
def get_procedures():
    with Session(engine) as session:
        statement = select(ProcMetadata).order_by(ProcMetadata.record_insert_datetime.desc())
        results = session.exec(statement).all()
        return results
