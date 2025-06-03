from fastapi import APIRouter, Depends, Query, Body, Request
from sqlalchemy.orm import Session
from sqlalchemy import text
import hashlib
import logging
import traceback
from app.core.database import get_db
from app.core.config import BRONZE_DB, SILVER_DB, GOLD_DB
from app.services.lineage.extract import extract_stage_to_bronze_mappings
from app.services.lineage.extract import extract_silver_gold_mappings
from app.services.lineage.persist import persist_silver_gold_mappings  # ensure it's only imported once
from app.services.lineage.agent import run_agent_query

# Helper function to extract column mappings from LLM given a procedure definition
def extract_column_mappings_from_llm(proc_definition: str):
    """
    Given a stored procedure definition, use LLM to extract column-level lineage mappings.
    Returns a tuple (mappings, error): mappings is a list of dicts, error is None if success, else error message.
    """
    try:
        from langchain_openai import AzureChatOpenAI
        from langchain.prompts import PromptTemplate
        import os
        import json
        import re

        llm = AzureChatOpenAI(
            azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_key=os.getenv("AZURE_OPENAI_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview"),
            temperature=0,
            max_tokens=2048,
        )

        prompt = PromptTemplate(
            input_variables=["proc_code"],
            template="""
You are an expert at SQL Server ETL lineage extraction.
Given this stored procedure, extract a JSON array of all column-level mappings in the format:

[
  {{
    "source_db": "...",
    "source_schema": "...",
    "source_table": "...",
    "source_column": "...",
    "target_db": "...",
    "target_schema": "...",
    "target_table": "...",
    "target_column": "...",
    "transform_expr": ""
  }}
]

If a mapping does not use a transform, leave transform_expr blank.

- `source_db` is the database the source table is read from (e.g. "wh_bronze").
- `source_schema` is the schema of the source table.
- `source_table` is the table name in the FROM clause.
- `source_column` is the original column.
- `target_db` is the database the procedure writes into (use the database context or variable if present; otherwise, infer based on naming).
- `target_schema` and `target_table` are the schema and table being inserted into.
- `target_column` is the destination column.
- `transform_expr` is any transformation expression applied to the column (otherwise blank).

Return ONLY the JSON array. Do not explain.

Procedure:
---
{proc_code}
---
"""
        )

        full_prompt = prompt.format(proc_code=proc_definition)
        result = llm.invoke(full_prompt)
        ai_text = getattr(result, "content", None) or str(result)

        # Remove triple backticks, language identifiers, and whitespace
        ai_text_clean = re.sub(r"^```(?:json)?|```$", "", ai_text.strip(), flags=re.MULTILINE).strip()
        # Try to find the JSON array
        match = re.search(r'\[[\s\S]*\]', ai_text_clean)
        if not match:
            logging.error(f"LLM lineage extraction failed. Raw response:\n{ai_text}")
            return None, "Could not find a JSON array in LLM output"
        json_str = match.group(0)
        try:
            mappings = json.loads(json_str)
        except Exception as ex:
            logging.error(f"Failed to parse LLM JSON: {ex}\nRaw string: {json_str}")
            return None, f"Failed to parse mappings JSON: {str(ex)}"
        return mappings, None
    except Exception as ex:
        logging.error(f"Exception in extract_column_mappings_from_llm: {ex}\n{traceback.format_exc()}")
        return None, f"Failed to analyze procedure: {str(ex)}"

router = APIRouter()
extract_router = router  # alias to expose extract_router

@router.get("/flat")
def get_flat_table_lineage(db: Session = Depends(get_db)):
    query = text("""
        SELECT
            lineage_id,
            stage_db, stage_schema, stage_table,
            bronze_db, bronze_schema, bronze_table,
            silver_db, silver_schema, silver_table,
            gold_db, gold_schema, gold_table
        FROM aud.vw_flat_table_lineage
        ORDER BY stage_db, stage_schema, stage_table,
            bronze_db, bronze_schema, bronze_table,
            silver_db, silver_schema, silver_table,
            gold_db, gold_schema, gold_table
    """)
    results = db.execute(query).mappings().all()
    return results

@router.post("/populate")
def populate_lineage_data(db: Session = Depends(get_db)):
    from .procs import run_full_lineage_population
    return run_full_lineage_population(db)

@router.get("/extract/bronze-to-silver")
def extract_bronze_to_silver(
    db: Session = Depends(get_db)
):
    query = text("""
        SELECT
            bronze_db, bronze_schema, bronze_table,
            silver_db, silver_schema, silver_table
        FROM aud.vw_flat_table_lineage
        WHERE bronze_db <> '' AND silver_db <> ''
    """)
    results = db.execute(query).mappings().all()
    return results

# New endpoint for extracting stage-to-bronze mappings
@router.get("/extract/stage-to-bronze")
def extract_stage_bronze_endpoint(
    db: Session = Depends(get_db),
    persist: bool = Query(default=False, description="If true, persists the mappings to aud.table_map and aud.table_source")
):
    mappings = extract_stage_to_bronze_mappings(db)
    if persist:
        from app.services.lineage.persist import persist_stage_to_bronze_mappings
        persist_stage_to_bronze_mappings(db, mappings)
    return mappings


# New endpoint for extracting silver-to-gold mappings
@router.get("/extract/silver-to-gold")
def extract_silver_gold_endpoint(
    db: Session = Depends(get_db),
    persist: bool = Query(default=False, description="If true, persists the mappings to aud.table_map")
):
    mappings = extract_silver_gold_mappings(db)
    if persist:
        persist_silver_gold_mappings(db, mappings)
    return mappings


# POST endpoint for persisting silver-to-gold mappings
@router.post("/extract/silver-to-gold")
async def persist_silver_gold_endpoint(
    db: Session = Depends(get_db),
    mappings: list[dict] = Body(default=[])
):
    if not mappings:
        return {"detail": "No mappings provided."}
    persist_silver_gold_mappings(db, mappings)
    return {"detail": f"{len(mappings)} mappings persisted."}


# GET endpoint to preview silver-to-gold stored procedures
@router.get("/extract/silver-to-gold/preview")
def preview_silver_gold_procs(db: Session = Depends(get_db)):
    query = text(f"""
        SELECT
            id AS proc_id,
            proc_name,
            source_db,
            source_schema,
            source_table
        FROM aud.proc_metadata
        WHERE source_db IN ('{BRONZE_DB}', '{SILVER_DB}')
    """)
    results = db.execute(query).mappings().all()
    return results


# POST endpoint to load all silver and gold tables into aud.table_source
@router.post("/load/silver-gold-tables")
def load_silver_gold_tables(db: Session = Depends(get_db)):
    query = text(f"""
        MERGE aud.table_source AS target
        USING (
            SELECT 
                '{SILVER_DB}' AS src_db, 
                table_schema AS src_schema, 
                table_name AS src_table
            FROM {SILVER_DB}.INFORMATION_SCHEMA.TABLES
            UNION
            SELECT 
                '{GOLD_DB}' AS src_db, 
                table_schema AS src_schema, 
                table_name AS src_table
            FROM {GOLD_DB}.INFORMATION_SCHEMA.TABLES
        ) AS src
        ON (
            target.src_db = src.src_db AND
            target.src_schema = src.src_schema AND
            target.src_table = src.src_table
        )
        WHEN NOT MATCHED THEN
            INSERT (src_db, src_schema, src_table, role, record_insert_datetime)
            VALUES (src.src_db, src.src_schema, src.src_table, 'destination', GETDATE());
    """)
    db.execute(query)
    db.commit()
    return {"detail": "Silver and gold tables loaded into aud.table_source."}

# GET endpoint to inspect what silver and gold tables were loaded
@router.get("/view/silver-gold-tables")
def view_silver_gold_tables(db: Session = Depends(get_db)):
    query = text(f"""
        SELECT
            src_db,
            src_schema,
            src_table,
            role,
            record_insert_datetime
        FROM aud.table_source
        WHERE src_db IN ('{SILVER_DB}', '{GOLD_DB}')
        ORDER BY src_db, src_schema, src_table
    """)
    results = db.execute(query).mappings().all()
    return results


# GET endpoint to extract all stored procedures from silver and gold databases
@router.get("/discover/silver-gold-procs")
def discover_silver_gold_procs(db: Session = Depends(get_db)):
    query = text(f"""
        SELECT
            1 as sort,
            p.name AS proc_name,
            s.name AS schema_name,
            m.definition AS proc_definition,
            '{SILVER_DB}' AS source_db
        FROM {SILVER_DB}.sys.procedures p
        JOIN {SILVER_DB}.sys.schemas s ON p.schema_id = s.schema_id
        JOIN {SILVER_DB}.sys.sql_modules m ON p.object_id = m.object_id
        UNION ALL
        SELECT
            2 as sort,
            p.name AS proc_name,
            s.name AS schema_name,
            m.definition AS proc_definition,
            '{GOLD_DB}' AS source_db
        FROM {GOLD_DB}.sys.procedures p
        JOIN {GOLD_DB}.sys.schemas s ON p.schema_id = s.schema_id
        JOIN {GOLD_DB}.sys.sql_modules m ON p.object_id = m.object_id
        Order By sort, schema_name, proc_name
    """)
    results = db.execute(query).mappings().all()

    import hashlib
    hashed_results = []
    for r in results:
        text_def = r["proc_definition"] or ""
        proc_hash = hashlib.sha256(text_def.encode("utf-8")).hexdigest()
        hashed_results.append({
            **r,
            "proc_hash": proc_hash
        })

    # Persist hashed_results into aud.proc_metadata, avoiding duplicates
    for row in hashed_results:
        db.execute(text(f"""
            MERGE aud.proc_metadata AS target
            USING (SELECT
                       :source_db AS source_db,
                       :schema_name AS source_schema,
                       :proc_name AS proc_name,
                       :proc_definition AS proc_definition,
                       :proc_hash AS proc_hash
                  ) AS src
            ON (target.source_db = src.source_db
                AND target.source_schema = src.source_schema
                AND target.proc_name = src.proc_name
                AND target.proc_hash = src.proc_hash)
            WHEN NOT MATCHED THEN
                INSERT (source_db, source_schema, proc_name, proc_definition, proc_hash, record_insert_datetime)
                VALUES (src.source_db, src.source_schema, src.proc_name, src.proc_definition, src.proc_hash, GETDATE());
        """), {
            "source_db": row["source_db"],
            "schema_name": row["schema_name"],
            "proc_name": row["proc_name"],
            "proc_definition": row["proc_definition"],
            "proc_hash": row["proc_hash"]
        })
    db.commit()

    return hashed_results


# GET endpoint to return stored procedure details by proc_hash
@router.get("/procedures/{proc_hash}")
def get_procedure_by_hash(proc_hash: str, db: Session = Depends(get_db)):
    query = text("""
        SELECT 
            proc_name,
            proc_hash,
            source_db,
            source_schema,
            proc_definition,
            record_insert_datetime
        FROM aud.proc_metadata
        WHERE proc_hash = :proc_hash
    """)
    result = db.execute(query, {"proc_hash": proc_hash}).mappings().first()
    if not result:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Procedure not found")
    return result

# GET endpoint to analyze a stored procedure with AI and extract table/column mappings
@router.get("/procedures/{proc_hash}/analyze")
def analyze_procedure(proc_hash: str, db: Session = Depends(get_db)):
    # 1. Get the stored proc by hash
    proc = db.execute(
        text("SELECT proc_name, proc_definition FROM aud.proc_metadata WHERE proc_hash = :proc_hash"),
        {"proc_hash": proc_hash}
    ).fetchone()
    if not proc:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Procedure not found")

    # 2. Use helper to extract mappings from LLM
    mappings, error = extract_column_mappings_from_llm(proc.proc_definition)
    if error:
        return {"error": error}

    # Post-process: inject target_db using the procedure's database context if blank
    proc_db = db.execute(
        text("SELECT source_db FROM aud.proc_metadata WHERE proc_hash = :proc_hash"),
        {"proc_hash": proc_hash}
    ).scalar() or ""
    for m in mappings:
        m["target_db"] = proc_db

    return {"mappings": mappings}

@router.post("/procedures/{proc_hash}/mappings")
def save_proc_mappings(proc_hash: str, mappings: list[dict] = Body(...), db: Session = Depends(get_db)):
    # 1. Lookup proc_metadata
    proc = db.execute(text("SELECT id, source_db FROM aud.proc_metadata WHERE proc_hash = :hash"), {"hash": proc_hash}).fetchone()
    if not proc:
        return {"error": "Procedure not found"}, 404

    proc_id = proc.id
    proc_db = proc.source_db or ""
    # Always force target_db to match proc context for all mappings
    for m in mappings:
        m["target_db"] = proc_db
        # TableMap for target
        result = db.execute(text("""
            SELECT id FROM aud.table_map
            WHERE proc_id = :proc_id
              AND dest_db = :dest_db
              AND dest_schema = :dest_schema
              AND dest_table = :dest_table
        """), {
            "proc_id": proc_id,
            "dest_db": m["target_db"],
            "dest_schema": m["target_schema"],
            "dest_table": m["target_table"]
        }).fetchone()
        if result:
            table_map_id = result.id
        else:
            ins = db.execute(text("""
                INSERT INTO aud.table_map (proc_id, dest_db, dest_schema, dest_table)
                OUTPUT INSERTED.id
                VALUES (:proc_id, :dest_db, :dest_schema, :dest_table)
            """), {
                "proc_id": proc_id,
                "dest_db": m["target_db"],
                "dest_schema": m["target_schema"],
                "dest_table": m["target_table"]
            })
            table_map_id = ins.scalar()
        # Insert into table_source (source table, role='source')
        db.execute(text("""
            IF NOT EXISTS (
                SELECT 1 FROM aud.table_source
                WHERE table_map_id = :table_map_id
                  AND src_db = :src_db
                  AND src_schema = :src_schema
                  AND src_table = :src_table
                  AND role = 'source'
            )
            INSERT INTO aud.table_source (table_map_id, src_db, src_schema, src_table, role)
            VALUES (:table_map_id, :src_db, :src_schema, :src_table, 'source')
        """), {
            "table_map_id": table_map_id,
            "src_db": m["source_db"],
            "src_schema": m["source_schema"],
            "src_table": m["source_table"]
        })
        # Insert into column_map
        db.execute(text("""
            INSERT INTO aud.column_map (table_source_id, dest_column, src_column, transform_expr)
            SELECT ts.id, :dest_column, :src_column, :transform_expr
            FROM aud.table_source ts
            WHERE ts.table_map_id = :table_map_id
              AND ts.src_db = :src_db
              AND ts.src_schema = :src_schema
              AND ts.src_table = :src_table
              AND ts.role = 'source'
        """), {
            "table_map_id": table_map_id,
            "src_db": m["source_db"],
            "src_schema": m["source_schema"],
            "src_table": m["source_table"],
            "dest_column": m["target_column"],
            "src_column": m["source_column"],
            "transform_expr": m.get("transform_expr", ""),
        })

    # After processing all mappings, update proc_metadata with first source_table (if any)
    if mappings:
        first_mapping = mappings[0]
        db.execute(
            text("""
                UPDATE aud.proc_metadata
                SET source_table = :source_table
                WHERE proc_hash = :proc_hash
            """),
            {
                "source_table": first_mapping.get("source_table", ""),
                "proc_hash": proc_hash,
            }
        )

    db.commit()
    return {"detail": "Mappings saved"}


# -------------------------------------------------------------
# Convenience endpoint: analyze all procs with the LLM and save mappings
# -------------------------------------------------------------
@router.post("/procedures/analyze-save-all")
def analyze_and_save_all_procedures(db: Session = Depends(get_db)):
    """
    Analyze and save lineage for all stored procedures in aud.proc_metadata.
    """
    # Fetch all stored procedure hashes and definitions
    proc_hashes = db.execute(
        text("SELECT proc_hash, proc_definition, source_db FROM aud.proc_metadata")
    ).mappings().all()

    results = []

    for row in proc_hashes:
        proc_hash = row["proc_hash"]
        proc_definition = row["proc_definition"]
        proc_db = row["source_db"] or ""
        try:
            mappings, error = extract_column_mappings_from_llm(proc_definition)
            if error:
                results.append({"proc_hash": proc_hash, "status": "error", "detail": error})
                continue
            for m in mappings:
                m["target_db"] = proc_db
            save_result = save_proc_mappings(proc_hash, mappings, db)
            if isinstance(save_result, tuple) and save_result[1] != 200:
                results.append({"proc_hash": proc_hash, "status": "error", "detail": save_result[0].get("error", "Save error")})
            else:
                results.append({"proc_hash": proc_hash, "status": "success", "detail": f"{len(mappings)} mappings saved."})
        except Exception as ex:
            results.append({"proc_hash": proc_hash, "status": "error", "detail": str(ex)})

    db.commit()
    return results


# -------------------------------------------------------------
# New endpoint: Accepts a natural language question and returns a SQL query with reasoning and lineage highlights
# -------------------------------------------------------------
from app.services.lineage.agent import run_agent_query

@router.post("/query/ai-sql")
async def ai_sql_agent(
    request: Request,
    question: str = Body(..., embed=True)
):
    """
    Accepts a natural language question and returns a SQL query with reasoning and lineage highlights.
    """
    response = run_agent_query(question)
    return response