from sqlalchemy import text
import os
from dotenv import load_dotenv
from sqlglot import exp
from sqlglot.lineage import lineage
from app.services.lineage.source_sqlserver import fetch_procedures
from app.services.lineage.models import ProcMetadata, TableMap, TableSource, ColumnMapping
from app.services.lineage.persist import insert_proc_metadata
from app.core.config import settings

load_dotenv()

STAGE_DB = settings.STAGE_DB
BRONZE_DB = settings.BRONZE_DB
SILVER_DB = settings.SILVER_DB
GOLD_DB = settings.GOLD_DB

def extract_stage_to_bronze_mappings(db):
    """
    Identify tables that exist in both stage and bronze layers based on table name matching.
    This mapping assumes consistent naming conventions across both layers.
    """
    if not STAGE_DB or not BRONZE_DB:
        raise ValueError("STAGE_DB or BRONZE_DB is not set in environment variables.")

    query = text(f"""
        SELECT 
            s.table_schema AS stage_schema,
            s.table_name AS stage_table_name,
            b.table_schema AS bronze_schema,
            b.table_name AS bronze_table_name
        FROM [{BRONZE_DB}].information_schema.tables b
        LEFT JOIN [{STAGE_DB}].information_schema.tables s
            ON s.table_name = b.table_name
    """)

    result = db.execute(query).fetchall()

    return [
        {
            "stage_schema": row.stage_schema,
            "stage_table_name": row.stage_table_name,
            "bronze_schema": row.bronze_schema,
            "bronze_table_name": row.bronze_table_name,
        }
        for row in result
    ]

def extract_table_sources_from_db(db, database_name):
    """
    Extract table sources from the given database's information_schema.tables.
    """
    query = text(f"""
        SELECT
            table_schema,
            table_name
        FROM [{database_name}].information_schema.tables
        WHERE table_type = 'BASE TABLE'
    """)
    result = db.execute(query).fetchall()
    return [
        {
            "src_db": database_name,
            "src_schema": row.table_schema,
            "src_table": row.table_name,
            "role": "destination"
        }
        for row in result
    ]

def extract_silver_table_sources(db):
    return extract_table_sources_from_db(db, SILVER_DB)

def extract_gold_table_sources(db):
    return extract_table_sources_from_db(db, GOLD_DB)

def extract_all_table_sources(db):
    """
    Extract table sources from all warehouse layers.
    """
    table_sources = []
    for db_name in [STAGE_DB, BRONZE_DB, SILVER_DB, GOLD_DB]:
        sources = extract_table_sources_from_db(db, db_name)
        table_sources.extend(sources)
    return table_sources

def extract_silver_gold_mappings(db):
    query = text(f"""
        SELECT
            pm.id AS proc_id,
            pm.proc_name,
            pm.source_db,
            pm.source_schema,
            pm.source_table,
            tm.dest_db,
            tm.dest_schema,
            tm.dest_table
        FROM aud.proc_metadata pm
        JOIN aud.table_map tm
          ON tm.proc_id = pm.id
        WHERE pm.source_db IN ('{settings.BRONZE_DB}', '{settings.SILVER_DB}')
    """)

    result = db.execute(query).fetchall()

    return [
        {
            "proc_id": row.proc_id,
            "proc_name": row.proc_name,
            "source_db": row.source_db,
            "source_schema": row.source_schema,
            "source_table": row.source_table,
            "dest_db": row.dest_db,
            "dest_schema": row.dest_schema,
            "dest_table": row.dest_table,
        }
        for row in result
    ]

if __name__ == "__main__":
    procedures = fetch_procedures(limit=10)

    for proc_name, proc_text in procedures:
        print(f"Processing: {proc_name}\n")

        lineage_node = lineage(column="*", sql=proc_text)

        lineage_sources = set()
        for node in lineage_node.walk():
            if node.source and isinstance(node.expression, exp.Column):
                table = node.expression.table
                lineage_sources.add((node.source_name or "unknown", table))

        table_sources = []
        for db, table in lineage_sources:
            table_sources.append(TableSource(
                src_db=db,
                src_schema="unknown",
                src_table=table,
                role="DESTINATION",
                columns=[],
            ))

        proc_model = ProcMetadata(
            proc_name=proc_name,
            proc_hash=str(hash(proc_text)),
            table_map=TableMap(
                dest_db="unknown",
                dest_schema="dbo",
                dest_table="unknown",
                sources=table_sources
            )
        )

        insert_proc_metadata(proc_model)