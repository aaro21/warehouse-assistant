ALTER VIEW [aud].[vw_flat_table_lineage] AS
WITH bronze_tables AS (
    SELECT
        tm.id AS bronze_table_map_id,
        tm.dest_db AS bronze_db,
        tm.dest_schema AS bronze_schema,
        tm.dest_table AS bronze_table
    FROM aud.table_map tm
    WHERE tm.dest_db LIKE '%bronze%'
)
SELECT
    ISNULL(st.src_db, '')     AS stage_db,
    ISNULL(st.src_schema, '') AS stage_schema,
    ISNULL(st.src_table, '')  AS stage_table,

    ISNULL(b.bronze_db, '')     AS bronze_db,
    ISNULL(b.bronze_schema, '') AS bronze_schema,
    ISNULL(b.bronze_table, '')  AS bronze_table,

    ISNULL(s.dest_db, '')     AS silver_db,
    ISNULL(s.dest_schema, '') AS silver_schema,
    ISNULL(s.dest_table, '')  AS silver_table,

    ISNULL(g.dest_db, '')     AS gold_db,
    ISNULL(g.dest_schema, '') AS gold_schema,
    ISNULL(g.dest_table, '')  AS gold_table,

    b.bronze_table_map_id AS lineage_id
FROM bronze_tables b
LEFT JOIN aud.table_source st
    ON b.bronze_table_map_id = st.table_map_id
    -- removed AND st.role = 'destination'

LEFT JOIN aud.table_map s
    ON s.proc_id IN (
        SELECT pm.id
        FROM aud.proc_metadata pm
        WHERE pm.source_db = b.bronze_db
          AND pm.source_schema = b.bronze_schema
          AND pm.source_table = b.bronze_table
    )
    AND s.dest_db = 'silver_db'

LEFT JOIN aud.table_map g
    ON g.proc_id IN (
        SELECT pm.id
        FROM aud.proc_metadata pm
        WHERE pm.source_db = s.dest_db
          AND pm.source_schema = s.dest_schema
          AND pm.source_table = s.dest_table
    )
    AND g.dest_db = 'gold_db';
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER VIEW [aud].[vw_flat_column_lineage] AS

-- Bronze columns that flow to silver
WITH bronze_columns AS (
    SELECT
        ts.table_map_id          AS table_map_id,
        ts.src_db                AS bronze_db,
        ts.src_schema            AS bronze_schema,
        ts.src_table             AS bronze_table,
        cm.src_column            AS bronze_column,
        tm.dest_db               AS silver_db,
        tm.dest_schema           AS silver_schema,
        tm.dest_table            AS silver_table,
        cm.dest_column           AS silver_column,
        cm.transform_expr,
        tm.proc_id               AS silver_proc_id
    FROM aud.table_source ts
    JOIN aud.column_map cm ON cm.table_source_id = ts.id
    JOIN aud.table_map tm ON ts.table_map_id = tm.id
    WHERE tm.dest_db LIKE '%silver%'
      AND ts.src_db LIKE '%bronze%'
)

-- Stage metadata: one row per table_map_id with a stage role
, stage_info AS (
    SELECT
        ts.table_map_id,
        ts.src_db     AS stage_db,
        ts.src_schema AS stage_schema,
        ts.src_table  AS stage_table
    FROM aud.table_source ts
    WHERE ts.src_db LIKE '%stage%'
      AND ts.role = 'source'
)

-- Silver to Gold flow
, silver_to_gold AS (
    SELECT
        ts.src_db      AS silver_db,
        ts.src_schema  AS silver_schema,
        ts.src_table   AS silver_table,
        cm.src_column  AS silver_column,
        tm.dest_db     AS gold_db,
        tm.dest_schema AS gold_schema,
        tm.dest_table  AS gold_table,
        cm.dest_column AS gold_column,
        cm.transform_expr,
        tm.proc_id     AS gold_proc_id
    FROM aud.table_source ts
    JOIN aud.column_map cm ON cm.table_source_id = ts.id
    JOIN aud.table_map tm ON ts.table_map_id = tm.id
    WHERE tm.dest_db LIKE '%gold%'
)

SELECT DISTINCT
    ISNULL(si.stage_db, '')       AS stage_db,
    ISNULL(si.stage_schema, '')   AS stage_schema,
    ISNULL(si.stage_table, '')    AS stage_table,
    ISNULL(bc.bronze_column, '')  AS stage_column,         -- ← Copy from bronze

    ISNULL(bc.bronze_db, '')      AS bronze_db,
    ISNULL(bc.bronze_schema, '')  AS bronze_schema,
    ISNULL(bc.bronze_table, '')   AS bronze_table,
    ISNULL(bc.bronze_column, '')  AS bronze_column,

    ISNULL(bc.silver_db, '')      AS silver_db,
    ISNULL(bc.silver_schema, '')  AS silver_schema,
    ISNULL(bc.silver_table, '')   AS silver_table,
    ISNULL(bc.silver_column, '')  AS silver_column,
    ISNULL(bc.transform_expr, '') AS silver_transform_expr,

    ISNULL(sg.gold_db, '')        AS gold_db,
    ISNULL(sg.gold_schema, '')    AS gold_schema,
    ISNULL(sg.gold_table, '')     AS gold_table,
    ISNULL(sg.gold_column, '')    AS gold_column,
    ISNULL(sg.transform_expr, '') AS gold_transform_expr

FROM bronze_columns bc
LEFT JOIN stage_info si
    ON si.stage_schema = bc.bronze_schema
    AND si.stage_table = bc.bronze_table
LEFT JOIN silver_to_gold sg
    ON bc.silver_db = sg.silver_db
   AND bc.silver_schema = sg.silver_schema
   AND bc.silver_table = sg.silver_table
   AND bc.silver_column = sg.silver_column
GO