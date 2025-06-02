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

CREATE OR ALTER VIEW [aud].[vw_flat_column_lineage] AS
-- Stage columns inferred from bronze
WITH bronze_columns AS (
    SELECT
        ts.id          AS table_source_id,
        ts.src_db      AS bronze_db,
        ts.src_schema  AS bronze_schema,
        ts.src_table   AS bronze_table,
        cm.src_column  AS bronze_column,
        tm.dest_db     AS silver_db,
        tm.dest_schema AS silver_schema,
        tm.dest_table  AS silver_table,
        cm.dest_column AS silver_column,
        cm.transform_expr,
        tm.proc_id     AS silver_proc_id
    FROM aud.table_source ts
    JOIN aud.column_map cm ON cm.table_source_id = ts.id
    JOIN aud.table_map tm ON ts.table_map_id = tm.id
    WHERE tm.dest_db LIKE '%silver%'
)
, stage_match AS (
    SELECT
        st.src_db     AS stage_db,
        st.src_schema AS stage_schema,
        st.src_table  AS stage_table,
        b.bronze_column AS stage_column,
        b.table_source_id
    FROM aud.table_source st
    JOIN bronze_columns b
        ON st.src_schema = b.bronze_schema
        AND st.src_table = b.bronze_table
    WHERE st.src_db NOT LIKE '%bronze%'
      AND st.src_db NOT LIKE '%silver%'
      AND st.src_db NOT LIKE '%gold%'
)
-- Silver to Gold columns
, silver_to_gold AS (
    SELECT
        ts.id          AS table_source_id,
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
SELECT
    ISNULL(sm.stage_db, '')       AS stage_db,
    ISNULL(sm.stage_schema, '')   AS stage_schema,
    ISNULL(sm.stage_table, '')    AS stage_table,
    ISNULL(sm.stage_column, '')   AS stage_column,

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
LEFT JOIN stage_match sm
    ON sm.table_source_id = bc.table_source_id
LEFT JOIN silver_to_gold sg
    ON bc.silver_db = sg.silver_db
   AND bc.silver_schema = sg.silver_schema
   AND bc.silver_table = sg.silver_table
   AND bc.silver_column = sg.silver_column
GO