from langchain_core.tools import tool
from langchain_core.tools import Tool
import os
from sqlmodel import Session
from sqlalchemy import text
from app.core.database import engine
from pydantic import BaseModel

class TableArgs(BaseModel):
    table_name: str
    schema_name: str = 'dbo'

# System message for agent
AGENT_SYSTEM_MESSAGE = '''
You are a helpful AI assistant specialized in data warehouse metadata and lineage analysis.

Use the following strategies when responding to user questions:

- If the user asks about a table (e.g., "Tell me about the silver dat_address table"), determine the layer (stage, bronze, silver, or gold), then use:
  - `get_table_info_*` tools to retrieve metadata about the table.
  - `get_column_info_*` tools to retrieve a list of columns and their data types.

- If the user asks about a column (e.g., "What is AddressID?"), use `get_column_lineage` to trace the column across stage, bronze, silver, and gold.

- Table and column names often change across layers. For example, a source table named "Customer" might appear as "dat_customer" or "dim_customer" in later layers. To find renamed tables or columns, use `search_lineage_view`.

- Table names often change across layers. To determine where a table appears or how it's named across layers, use `search_table_lineage_view`.

- To determine which layer(s) a table or column exists in, use `search_lineage_view`.

Prioritize using these tools before forming your answer. Do not rely on assumptions when metadata or lineage can be queried directly.
'''

# Tool to resolve table name variants across layers
@tool
def resolve_table_variants(keyword: str) -> str:
    """
    Given a keyword, searches vw_flat_table_lineage for related table names across layers.
    Returns a deduplicated list of actual table names that match the keyword.
    """
    with Session(engine) as session:
        query = text("""
            SELECT DISTINCT LOWER(stage_schema) as schema_name, LOWER(stage_table) as table_name FROM aud.vw_flat_table_lineage WHERE LOWER(stage_table) LIKE :kw
            UNION
            SELECT DISTINCT LOWER(bronze_schema), LOWER(bronze_table) FROM aud.vw_flat_table_lineage WHERE LOWER(bronze_table) LIKE :kw
            UNION
            SELECT DISTINCT LOWER(silver_schema), LOWER(silver_table) FROM aud.vw_flat_table_lineage WHERE LOWER(silver_table) LIKE :kw
            UNION
            SELECT DISTINCT LOWER(gold_schema), LOWER(gold_table) FROM aud.vw_flat_table_lineage WHERE LOWER(gold_table) LIKE :kw
        """)
        result = session.execute(query, {"kw": f"%{keyword.lower()}%"}).fetchall()
        if not result:
            return f"No table name variants found for keyword: {keyword}"
        return "\n".join(sorted(set(f"{row[0]}.{row[1]}" for row in result if row[0] and row[1])))

@tool
def get_info_for_table_variants(keyword: str) -> str:
    """
    Resolves table name variants using lineage view, then retrieves table metadata for each match (now includes schema name).
    """
    with Session(engine) as session:
        variant_query = text("""
            SELECT DISTINCT LOWER(stage_schema) as schema_name, LOWER(stage_table) as table_name FROM aud.vw_flat_table_lineage WHERE LOWER(stage_table) LIKE :kw
            UNION
            SELECT DISTINCT LOWER(bronze_schema), LOWER(bronze_table) FROM aud.vw_flat_table_lineage WHERE LOWER(bronze_table) LIKE :kw
            UNION
            SELECT DISTINCT LOWER(silver_schema), LOWER(silver_table) FROM aud.vw_flat_table_lineage WHERE LOWER(silver_table) LIKE :kw
            UNION
            SELECT DISTINCT LOWER(gold_schema), LOWER(gold_table) FROM aud.vw_flat_table_lineage WHERE LOWER(gold_table) LIKE :kw
        """)
        results = session.execute(variant_query, {"kw": f"%{keyword.lower()}%"}).fetchall()
        # set of (table_name, schema_name) pairs, skip any where table_name is None/empty
        table_schema_pairs = sorted(set(
            (row[1], row[0]) for row in results if row[1] and row[0]
        ))

        if not table_schema_pairs:
            return f"No table name variants found for keyword: {keyword}"

        combined_info = []
        for table_name, schema_name in table_schema_pairs:
            info = get_table_info_all_layers(table_name, schema_name)
            combined_info.append(f"== {schema_name}.{table_name.upper()} ==\n{info}")
        return "\n\n".join(combined_info)

@tool
def get_column_lineage(column_name: str) -> str:
    """Returns a verbose breakdown of the lineage path for a given column, including stage, bronze, silver, and gold layers."""
    with Session(engine) as session:
        query = text("""
            SELECT
                stage_db, stage_schema, stage_table, stage_column,
                bronze_db, bronze_schema, bronze_table, bronze_column,
                silver_db, silver_schema, silver_table, silver_column, silver_transform_expr,
                gold_db, gold_schema, gold_table, gold_column, gold_transform_expr
            FROM aud.vw_flat_column_lineage
            WHERE
                LOWER(stage_column) = LOWER(:column) OR
                LOWER(bronze_column) = LOWER(:column) OR
                LOWER(silver_column) = LOWER(:column) OR
                LOWER(gold_column) = LOWER(:column)
        """)
        result = session.execute(query, {"column": column_name}).fetchall()

        if not result:
            return f"No lineage found for column '{column_name}'"

        lines = []
        for row in result:
            lines.append(f"""
───── {column_name} LINEAGE ─────
Stage:  {row.stage_db}.{row.stage_schema}.{row.stage_table}.{row.stage_column}
Bronze: {row.bronze_db}.{row.bronze_schema}.{row.bronze_table}.{row.bronze_column}
Silver: {row.silver_db}.{row.silver_schema}.{row.silver_table}.{row.silver_column}
  ↳ Transform: {row.silver_transform_expr}
Gold:   {row.gold_db}.{row.gold_schema}.{row.gold_table}.{row.gold_column}
  ↳ Transform: {row.gold_transform_expr}
            """.strip())

        return "\n\n".join(lines)


# Helper function to run SQL query and return formatted result
def run_sql_query(query: str) -> str:
    with Session(engine) as session:
        result = session.execute(text(query)).fetchall()
        if not result:
            return "No results found."
        return "\n".join([str(dict(row._mapping)) for row in result])


def get_column_info_stage(table_name: str, schema_name: str = 'dbo') -> str:
    return run_sql_query(
        f"SELECT * FROM {os.getenv('STAGE_DB')}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE LOWER(TABLE_NAME) = LOWER('{table_name}') AND LOWER(TABLE_SCHEMA) = LOWER('{schema_name}')"
    )

def get_column_info_bronze(table_name: str, schema_name: str = 'dbo') -> str:
    return run_sql_query(
        f"SELECT * FROM {os.getenv('BRONZE_DB')}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE LOWER(TABLE_NAME) = LOWER('{table_name}') AND LOWER(TABLE_SCHEMA) = LOWER('{schema_name}')"
    )

def get_column_info_silver(table_name: str, schema_name: str = 'dbo') -> str:
    return run_sql_query(
        f"SELECT * FROM {os.getenv('SILVER_DB')}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE LOWER(TABLE_NAME) = LOWER('{table_name}') AND LOWER(TABLE_SCHEMA) = LOWER('{schema_name}')"
    )

def get_column_info_gold(table_name: str, schema_name: str = 'dbo') -> str:
    return run_sql_query(
        f"SELECT * FROM {os.getenv('GOLD_DB')}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE LOWER(TABLE_NAME) = LOWER('{table_name}') AND LOWER(TABLE_SCHEMA) = LOWER('{schema_name}')"
    )


# Table info functions
def get_table_info_stage(table_name: str, schema_name: str = 'dbo') -> str:
    return run_sql_query(
        f"SELECT * FROM {os.getenv('STAGE_DB')}.INFORMATION_SCHEMA.TABLES "
        f"WHERE LOWER(TABLE_NAME) = LOWER('{table_name}') AND LOWER(TABLE_SCHEMA) = LOWER('{schema_name}')"
    )

def get_table_info_bronze(table_name: str, schema_name: str = 'dbo') -> str:
    return run_sql_query(
        f"SELECT * FROM {os.getenv('BRONZE_DB')}.INFORMATION_SCHEMA.TABLES "
        f"WHERE LOWER(TABLE_NAME) = LOWER('{table_name}') AND LOWER(TABLE_SCHEMA) = LOWER('{schema_name}')"
    )

def get_table_info_silver(table_name: str, schema_name: str = 'dbo') -> str:
    return run_sql_query(
        f"SELECT * FROM {os.getenv('SILVER_DB')}.INFORMATION_SCHEMA.TABLES "
        f"WHERE LOWER(TABLE_NAME) = LOWER('{table_name}') AND LOWER(TABLE_SCHEMA) = LOWER('{schema_name}')"
    )

def get_table_info_gold(table_name: str, schema_name: str = 'dbo') -> str:
    return run_sql_query(
        f"SELECT * FROM {os.getenv('GOLD_DB')}.INFORMATION_SCHEMA.TABLES "
        f"WHERE LOWER(TABLE_NAME) = LOWER('{table_name}') AND LOWER(TABLE_SCHEMA) = LOWER('{schema_name}')"
    )

def get_table_info_all_layers(table_name: str, schema_name: str = 'dbo') -> str:
    queries = [
        f"SELECT 'stage' AS layer, * FROM {os.getenv('STAGE_DB')}.INFORMATION_SCHEMA.TABLES WHERE LOWER(TABLE_NAME) = LOWER('{table_name}') AND LOWER(TABLE_SCHEMA) = LOWER('{schema_name}')",
        f"SELECT 'bronze' AS layer, * FROM {os.getenv('BRONZE_DB')}.INFORMATION_SCHEMA.TABLES WHERE LOWER(TABLE_NAME) = LOWER('{table_name}') AND LOWER(TABLE_SCHEMA) = LOWER('{schema_name}')",
        f"SELECT 'silver' AS layer, * FROM {os.getenv('SILVER_DB')}.INFORMATION_SCHEMA.TABLES WHERE LOWER(TABLE_NAME) = LOWER('{table_name}') AND LOWER(TABLE_SCHEMA) = LOWER('{schema_name}')",
        f"SELECT 'gold' AS layer, * FROM {os.getenv('GOLD_DB')}.INFORMATION_SCHEMA.TABLES WHERE LOWER(TABLE_NAME) = LOWER('{table_name}') AND LOWER(TABLE_SCHEMA) = LOWER('{schema_name}')"
    ]
    combined_results = []
    with Session(engine) as session:
        for query in queries:
            result = session.execute(text(query)).fetchall()
            if result:
                for row in result:
                    combined_results.append(str(dict(row._mapping)))
    if not combined_results:
        return "No results found."
    return "\n".join(combined_results)


@tool
def search_lineage_view(keyword: str) -> str:
    """
    Search the vw_flat_column_lineage view for any table or column names that match the given keyword.
    Helps determine which layer(s) a table or column appears in.
    """
    with Session(engine) as session:
        query = text(f"""
            SELECT
                stage_db, stage_schema, stage_table, stage_column,
                bronze_db, bronze_schema, bronze_table, bronze_column,
                silver_db, silver_schema, silver_table, silver_column,
                gold_db, gold_schema, gold_table, gold_column
            FROM aud.vw_flat_column_lineage
            WHERE
                LOWER(stage_table) LIKE :kw OR LOWER(stage_column) LIKE :kw OR
                LOWER(bronze_table) LIKE :kw OR LOWER(bronze_column) LIKE :kw OR
                LOWER(silver_table) LIKE :kw OR LOWER(silver_column) LIKE :kw OR
                LOWER(gold_table) LIKE :kw OR LOWER(gold_column) LIKE :kw
        """)
        result = session.execute(query, {"kw": f"%{keyword.lower()}%"}).fetchall()
        if not result:
            return f"No matches found in vw_flat_column_lineage for keyword: {keyword}"
        return "\n".join([str(dict(row)) for row in result])


@tool
def search_table_lineage_view(keyword: str) -> str:
    """
    Search the vw_flat_table_lineage view for any table names that match the given keyword.
    Helps determine which layer(s) a table appears in.
    """
    with Session(engine) as session:
        query = text("""
            SELECT
                stage_db, stage_schema, stage_table,
                bronze_db, bronze_schema, bronze_table,
                silver_db, silver_schema, silver_table,
                gold_db, gold_schema, gold_table,
                lineage_id
            FROM aud.vw_flat_table_lineage
            WHERE
                LOWER(stage_table) LIKE :kw OR
                LOWER(bronze_table) LIKE :kw OR
                LOWER(silver_table) LIKE :kw OR
                LOWER(gold_table) LIKE :kw
        """)
        result = session.execute(query, {"kw": f"%{keyword.lower()}%"}).fetchall()
        if not result:
            return f"No matches found in vw_flat_table_lineage for keyword: {keyword}"
        return "\n".join([str(dict(row._mapping)) for row in result])


@tool
def get_column_info_stage_single(table: str) -> str:
    """Get column metadata for a STAGE table. Provide input as 'table_name;schema_name'."""
    table_name, schema_name = (table.split(";") + ['dbo'])[:2]
    return get_column_info_stage(table_name.strip(), schema_name.strip())

@tool
def get_column_info_bronze_single(table: str) -> str:
    """Get column metadata for a BRONZE table. Provide input as 'table_name;schema_name'."""
    table_name, schema_name = (table.split(";") + ['dbo'])[:2]
    return get_column_info_bronze(table_name.strip(), schema_name.strip())

@tool
def get_column_info_silver_single(table: str) -> str:
    """Get column metadata for a SILVER table. Provide input as 'table_name;schema_name'."""
    table_name, schema_name = (table.split(";") + ['dbo'])[:2]
    return get_column_info_silver(table_name.strip(), schema_name.strip())

@tool
def get_column_info_gold_single(table: str) -> str:
    """Get column metadata for a GOLD table. Provide input as 'table_name;schema_name'."""
    table_name, schema_name = (table.split(";") + ['dbo'])[:2]
    return get_column_info_gold(table_name.strip(), schema_name.strip())

@tool
def get_table_info_stage_single(table: str) -> str:
    """Get table metadata for a STAGE table. Provide input as 'table_name;schema_name'."""
    table_name, schema_name = (table.split(";") + ['dbo'])[:2]
    return get_table_info_stage(table_name.strip(), schema_name.strip())

@tool
def get_table_info_bronze_single(table: str) -> str:
    """Get table metadata for a BRONZE table. Provide input as 'table_name;schema_name'."""
    table_name, schema_name = (table.split(";") + ['dbo'])[:2]
    return get_table_info_bronze(table_name.strip(), schema_name.strip())

@tool
def get_table_info_silver_single(table: str) -> str:
    """Get table metadata for a SILVER table. Provide input as 'table_name;schema_name'."""
    table_name, schema_name = (table.split(";") + ['dbo'])[:2]
    return get_table_info_silver(table_name.strip(), schema_name.strip())

@tool
def get_table_info_gold_single(table: str) -> str:
    """Get metadata for a single GOLD layer table, specified as 'table_name;schema_name'."""
    table_name, schema_name = (table.split(";") + ['dbo'])[:2]
    return get_table_info_gold(table_name.strip(), schema_name.strip())


# List of tools to register with the agent
tools = [
    get_column_lineage,
    get_column_info_stage_single,
    get_column_info_bronze_single,
    get_column_info_silver_single,
    get_column_info_gold_single,
    get_table_info_stage_single,
    get_table_info_bronze_single,
    get_table_info_silver_single,
    get_table_info_gold_single,
    Tool(
        name="get_table_info_all_layers",
        func=get_table_info_all_layers,
        description="Get metadata for a table from all layers: STAGE, BRONZE, SILVER, and GOLD."
    ),
    search_lineage_view,
    search_table_lineage_view,
    resolve_table_variants,
    get_info_for_table_variants,
]