-- Clear existing data
DELETE FROM aud.column_map;
DELETE FROM aud.table_source;
DELETE FROM aud.table_map;
DELETE FROM aud.proc_metadata;

-- Insert proc_metadata (only for Silver and Gold stages)
SET IDENTITY_INSERT aud.proc_metadata ON;
INSERT INTO aud.proc_metadata (id, proc_name, proc_hash, source_db, source_schema, source_table, record_insert_datetime)
VALUES
  (1, 'usp_bronze_to_silver_sales_orders', 'def456', 'bronze_db', 'sales', 'orders', GETDATE()),
  (2, 'usp_silver_to_gold_sales_orders',  'ghi789', 'silver_db', 'sales', 'dat_orders', GETDATE()),
  (3, 'usp_bronze_to_silver_sales_customers', 'def789', 'bronze_db', 'sales', 'customers', GETDATE()),
  (4, 'usp_silver_to_gold_sales_customers',  'ghi321', 'silver_db', 'sales', 'dat_customers', GETDATE());
SET IDENTITY_INSERT aud.proc_metadata OFF;

-- Insert table_map (Stage, Bronze, Silver, Gold for 2 datasets)
SET IDENTITY_INSERT aud.table_map ON;
-- Orders flow
INSERT INTO aud.table_map (id, proc_id, dest_db, dest_schema, dest_table, record_insert_datetime)
VALUES
  (1, NULL, 'source_db', 'sales', 'orders_raw', GETDATE()),      -- Stage
  (2, NULL, 'bronze_db', 'sales', 'orders', GETDATE()),          -- Bronze
  (3, 1,    'silver_db', 'sales', 'dat_orders', GETDATE()),      -- Silver
  (4, 2,    'gold_db',   'sales', 'dim_orders', GETDATE());      -- Gold

-- Customers flow
INSERT INTO aud.table_map (id, proc_id, dest_db, dest_schema, dest_table, record_insert_datetime)
VALUES
  (5, NULL, 'source_db', 'sales', 'customers_raw', GETDATE()),   -- Stage
  (6, NULL, 'bronze_db', 'sales', 'customers', GETDATE()),       -- Bronze
  (7, 3,    'silver_db', 'sales', 'dat_customers', GETDATE()),   -- Silver
  (8, 4,    'gold_db',   'sales', 'dim_customers', GETDATE());   -- Gold
SET IDENTITY_INSERT aud.table_map OFF;

-- Link table_source back through lineage
SET IDENTITY_INSERT aud.table_source ON;
-- Orders flow
INSERT INTO aud.table_source (id, table_map_id, src_db, src_schema, src_table, role, join_predicate, record_insert_datetime)
VALUES
  (1, 2, 'source_db', 'sales', 'orders_raw', 'destination', NULL, GETDATE()),
  (2, 3, 'bronze_db', 'sales', 'orders', 'destination', NULL, GETDATE()),
  (3, 4, 'silver_db', 'sales', 'dat_orders', 'destination', NULL, GETDATE());

-- Customers flow
INSERT INTO aud.table_source (id, table_map_id, src_db, src_schema, src_table, role, join_predicate, record_insert_datetime)
VALUES
  (4, 6, 'source_db', 'sales', 'customers_raw', 'destination', NULL, GETDATE()),
  (5, 7, 'bronze_db', 'sales', 'customers', 'destination', NULL, GETDATE()),
  (6, 8, 'silver_db', 'sales', 'dat_customers', 'destination', NULL, GETDATE());
SET IDENTITY_INSERT aud.table_source OFF;

-- Column mappings (keep simple for now)
SET IDENTITY_INSERT aud.column_map ON;
-- Orders flow
INSERT INTO aud.column_map (id, table_source_id, dest_column, src_column, transform_expr, record_insert_datetime)
VALUES
  (1, 1, 'order_id', 'id', NULL, GETDATE()),
  (2, 2, 'order_total', 'total', 'CAST(total AS DECIMAL(10,2))', GETDATE()),
  (3, 3, 'order_status', 'status', NULL, GETDATE());

-- Customers flow
INSERT INTO aud.column_map (id, table_source_id, dest_column, src_column, transform_expr, record_insert_datetime)
VALUES
  (4, 4, 'customer_id', 'id', NULL, GETDATE()),
  (5, 5, 'customer_name', 'name', NULL, GETDATE()),
  (6, 6, 'customer_type', 'type', NULL, GETDATE());
SET IDENTITY_INSERT aud.column_map OFF;