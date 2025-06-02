from sqlmodel import Session, SQLModel, create_engine
from app.services.lineage.models_sql import ProcMetadata as SQLProcMetadata, TableMap as SQLTableMap, TableSource as SQLTableSource, ColumnMap as SQLColumnMap
from app.services.lineage.models import ProcMetadata
from datetime import datetime
import os

from sqlalchemy.engine import URL

from sqlalchemy import text

from app.core.config import STAGE_DB, BRONZE_DB, SILVER_DB, GOLD_DB

# Configure the engine for SQL Server
url = URL.create(
    "mssql+pyodbc",
    username=os.getenv("SQL_USER"),
    password=os.getenv("SQL_PASSWORD"),
    host=os.getenv("SQL_SERVER"),
    database=os.getenv("SQL_DB"),
    query={"driver": "ODBC Driver 18 for SQL Server"},
)
engine = create_engine(url)


def insert_proc_metadata(proc_data: ProcMetadata):
    with Session(engine) as session:
        # Insert ProcMetadata
        db_proc = SQLProcMetadata(
            proc_name=proc_data.proc_name,
            proc_hash=proc_data.proc_hash,
            record_insert_datetime=proc_data.record_insert_datetime or datetime.utcnow(),
        )
        session.add(db_proc)
        session.flush()  # get db_proc.id

        # Insert TableMap
        db_table_map = SQLTableMap(
            proc_id=db_proc.id,
            dest_db=proc_data.table_map.dest_db,
            dest_schema=proc_data.table_map.dest_schema,
            dest_table=proc_data.table_map.dest_table,
        )
        session.add(db_table_map)
        session.flush()

        # Insert TableSources and ColumnMaps
        for source in proc_data.table_map.sources:
            db_source = SQLTableSource(
                table_map_id=db_table_map.id,
                src_db=source.src_db,
                src_schema=source.src_schema,
                src_table=source.src_table,
                role=source.role,
                join_predicate=source.join_predicate,
            )
            session.add(db_source)
            session.flush()

            for col in source.columns:
                db_col = SQLColumnMap(
                    table_source_id=db_source.id,
                    dest_column=col.dest_column,
                    src_column=col.src_column,
                    transform_expr=col.transform_expr,
                )
                session.add(db_col)

        session.commit()



# New function to persist stage to bronze mappings (refactored)
def persist_stage_to_bronze_mappings(db, mappings: list[dict]):
    for mapping in mappings:
        # Always insert bronze as the destination (anchor)
        result = db.execute(text("""
            SELECT id FROM aud.table_map
            WHERE dest_db = :dest_db AND dest_schema = :dest_schema AND dest_table = :dest_table
        """), {
            "dest_db": BRONZE_DB,
            "dest_schema": mapping["bronze_schema"],
            "dest_table": mapping["bronze_table_name"],
        })
        row = result.fetchone()

        if row:
            table_map_id = row.id
        else:
            # Insert new record into table_map and capture the inserted ID
            result = db.execute(text("""
                INSERT INTO aud.table_map (dest_db, dest_schema, dest_table)
                OUTPUT INSERTED.id
                VALUES (:dest_db, :dest_schema, :dest_table)
            """), {
                "dest_db": BRONZE_DB,
                "dest_schema": mapping["bronze_schema"],
                "dest_table": mapping["bronze_table_name"],
            })
            table_map_id = result.scalar()

        # Insert bronze as destination if not already linked
        exists_bronze = db.execute(text("""
            SELECT 1 FROM aud.table_source
            WHERE table_map_id = :table_map_id
            AND src_db = :src_db
            AND src_schema = :src_schema
            AND src_table = :src_table
            AND role = 'destination'
        """), {
            "table_map_id": table_map_id,
            "src_db": BRONZE_DB,
            "src_schema": mapping["bronze_schema"],
            "src_table": mapping["bronze_table_name"],
        }).fetchone()
        if not exists_bronze:
            db.execute(text("""
                INSERT INTO aud.table_source (table_map_id, src_db, src_schema, src_table, role)
                VALUES (:table_map_id, :src_db, :src_schema, :src_table, 'destination')
            """), {
                "table_map_id": table_map_id,
                "src_db": BRONZE_DB,
                "src_schema": mapping["bronze_schema"],
                "src_table": mapping["bronze_table_name"],
            })

        # Insert stage as source if present and not already linked
        if mapping["stage_table_name"]:
            exists_stage = db.execute(text("""
                SELECT 1 FROM aud.table_source
                WHERE table_map_id = :table_map_id
                AND src_db = :src_db
                AND src_schema = :src_schema
                AND src_table = :src_table
                AND role = 'source'
            """), {
                "table_map_id": table_map_id,
                "src_db": STAGE_DB,
                "src_schema": mapping["stage_schema"],
                "src_table": mapping["stage_table_name"],
            }).fetchone()
            if not exists_stage:
                db.execute(text("""
                    INSERT INTO aud.table_source (table_map_id, src_db, src_schema, src_table, role)
                    VALUES (:table_map_id, :src_db, :src_schema, :src_table, 'source')
                """), {
                    "table_map_id": table_map_id,
                    "src_db": STAGE_DB,
                    "src_schema": mapping["stage_schema"],
                    "src_table": mapping["stage_table_name"],
                })

    db.commit()



# New function to persist silver and gold mappings
def persist_silver_gold_mappings(db, mappings: list[dict]):
    inserted_count = 0

    for mapping in mappings:
        # Check if destination table already exists in table_map for the given proc_id
        result = db.execute(text("""
            SELECT 1 FROM aud.table_map
            WHERE proc_id = :proc_id AND dest_db = :dest_db AND dest_schema = :dest_schema AND dest_table = :dest_table
        """), {
            "proc_id": mapping["proc_id"],
            "dest_db": mapping["dest_db"],
            "dest_schema": mapping["dest_schema"],
            "dest_table": mapping["dest_table"],
        }).fetchone()

        if not result:
            db.execute(text("""
                INSERT INTO aud.table_map (proc_id, dest_db, dest_schema, dest_table)
                VALUES (:proc_id, :dest_db, :dest_schema, :dest_table)
            """), {
                "proc_id": mapping["proc_id"],
                "dest_db": mapping["dest_db"],
                "dest_schema": mapping["dest_schema"],
                "dest_table": mapping["dest_table"],
            })
            inserted_count += 1

    db.commit()
    return inserted_count


# Function to persist all extracted table sources (stage, bronze, silver, and gold) into aud.table_source
def persist_all_table_sources(db, table_sources: list[dict]):
    inserted_count = 0

    for src in table_sources:
        # Check if this table source already exists for the given role
        exists = db.execute(text("""
            SELECT 1 FROM aud.table_source
            WHERE table_map_id = :table_map_id
            AND src_db = :src_db
            AND src_schema = :src_schema
            AND src_table = :src_table
            AND role = :role
        """), {
            "table_map_id": src["table_map_id"],
            "src_db": src["src_db"],
            "src_schema": src["src_schema"],
            "src_table": src["src_table"],
            "role": src["role"]
        }).fetchone()

        if not exists:
            db.execute(text("""
                INSERT INTO aud.table_source (table_map_id, src_db, src_schema, src_table, role, join_predicate)
                VALUES (:table_map_id, :src_db, :src_schema, :src_table, :role, :join_predicate)
            """), {
                "table_map_id": src["table_map_id"],
                "src_db": src["src_db"],
                "src_schema": src["src_schema"],
                "src_table": src["src_table"],
                "role": src["role"],
                "join_predicate": src.get("join_predicate"),
            })
            inserted_count += 1

    db.commit()
    return inserted_count
def persist_silver_gold_tables(db):
    inserted_count = 0

    for db_name in [SILVER_DB, GOLD_DB]:
        tables = db.execute(text(f"""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM {db_name}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
        """)).fetchall()

        for table in tables:
            dest_schema = table.TABLE_SCHEMA
            dest_table = table.TABLE_NAME

            # Check if already exists in table_map
            result = db.execute(text("""
                SELECT id FROM aud.table_map
                WHERE dest_db = :dest_db AND dest_schema = :dest_schema AND dest_table = :dest_table
            """), {
                "dest_db": db_name,
                "dest_schema": dest_schema,
                "dest_table": dest_table,
            }).fetchone()

            if result:
                table_map_id = result.id
            else:
                # Insert into table_map
                result = db.execute(text("""
                    INSERT INTO aud.table_map (dest_db, dest_schema, dest_table)
                    OUTPUT INSERTED.id
                    VALUES (:dest_db, :dest_schema, :dest_table)
                """), {
                    "dest_db": db_name,
                    "dest_schema": dest_schema,
                    "dest_table": dest_table,
                })
                table_map_id = result.scalar()
                inserted_count += 1

            # Check if already exists in table_source
            exists = db.execute(text("""
                SELECT 1 FROM aud.table_source
                WHERE table_map_id = :table_map_id
                AND src_db = :src_db
                AND src_schema = :src_schema
                AND src_table = :src_table
                AND role = 'PRIMARY'
            """), {
                "table_map_id": table_map_id,
                "src_db": db_name,
                "src_schema": dest_schema,
                "src_table": dest_table,
            }).fetchone()

            if not exists:
                db.execute(text("""
                    INSERT INTO aud.table_source (table_map_id, src_db, src_schema, src_table, role)
                    VALUES (:table_map_id, :src_db, :src_schema, :src_table, 'PRIMARY')
                """), {
                    "table_map_id": table_map_id,
                    "src_db": db_name,
                    "src_schema": dest_schema,
                    "src_table": dest_table,
                })

    db.commit()
    return inserted_count