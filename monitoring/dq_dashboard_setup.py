# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC #### DQ Dashboard — View Setup
# MAGIC Run once per environment (dev, then again for uat/prod once they exist),
# MAGIC same as `03_event_log_setup.py`. Creates the 3 views backing the DQ dashboard,
# MAGIC scoped to the given `catalog` — a fresh, correctly-qualified set of views per
# MAGIC environment, not one view trying to dynamically switch catalogs at query time.
# MAGIC
# MAGIC Requires `03_event_log_setup.py` to have already been run for this catalog.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev", "Target catalog")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Expectations, Bronze and Silver, unified
# MAGIC Same query shape covers both layers — expectations are expectations, regardless
# MAGIC of which pipeline wrote them. 

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.stepright.dq_expectations_history AS
SELECT
  origin.pipeline_name,
  origin.flow_name AS table_name,
  timestamp,
  exp.col.name AS rule_name,
  exp.col.dataset,
  exp.col.passed_records,
  exp.col.failed_records,
  ROUND(exp.col.failed_records / NULLIF(exp.col.passed_records + exp.col.failed_records, 0) * 100, 2) AS failure_rate_pct
FROM {catalog}.stepright.ingestion_event_log_raw
CROSS JOIN LATERAL EXPLODE(
  FROM_JSON(details:flow_progress.data_quality.expectations,
    'array<struct<name:string, dataset:string, passed_records:int, failed_records:int>>')
) AS exp
WHERE event_type = 'flow_progress'

UNION ALL

SELECT
  origin.pipeline_name,
  origin.flow_name AS table_name,
  timestamp,
  exp.col.name AS rule_name,
  exp.col.dataset,
  exp.col.passed_records,
  exp.col.failed_records,
  ROUND(exp.col.failed_records / NULLIF(exp.col.passed_records + exp.col.failed_records, 0) * 100, 2) AS failure_rate_pct
FROM {catalog}.stepright.transformation_event_log_raw
CROSS JOIN LATERAL EXPLODE(
  FROM_JSON(details:flow_progress.data_quality.expectations,
    'array<struct<name:string, dataset:string, passed_records:int, failed_records:int>>')
) AS exp
WHERE event_type = 'flow_progress'
""")

print(f"Created {catalog}.stepright.dq_expectations_history")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.stepright.dq_expectations_history

# COMMAND ----------

# MAGIC %md
# MAGIC ####Quarantine-table monitoring, all 8 pairs (Bronze's 7 + Silver's 1)
# MAGIC Direct row-count queries against the tables themselves, not the event log —
# MAGIC these tables exist independently of any single pipeline run.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.stepright.dq_quarantine_counts AS
SELECT 'bronze_orders' AS source, COUNT(*) AS quarantined_count, current_timestamp() AS checked_at FROM {catalog}.stepright.bronze_orders_quarantined
UNION ALL SELECT 'bronze_order_items', COUNT(*), current_timestamp() FROM {catalog}.stepright.bronze_order_items_quarantined
UNION ALL SELECT 'bronze_customers', COUNT(*), current_timestamp() FROM {catalog}.stepright.bronze_customers_quarantined
UNION ALL SELECT 'bronze_products', COUNT(*), current_timestamp() FROM {catalog}.stepright.bronze_products_quarantined
UNION ALL SELECT 'bronze_categories', COUNT(*), current_timestamp() FROM {catalog}.stepright.bronze_categories_quarantined
UNION ALL SELECT 'bronze_clickstream', COUNT(*), current_timestamp() FROM {catalog}.stepright.bronze_clickstream_quarantined
UNION ALL SELECT 'bronze_inventory', COUNT(*), current_timestamp() FROM {catalog}.stepright.bronze_inventory_quarantined
UNION ALL SELECT 'silver_order_items', COUNT(*), current_timestamp() FROM {catalog}.stepright.silver_order_items_quarantined
""")

print(f"Created {catalog}.stepright.dq_quarantine_counts")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.stepright.dq_quarantine_counts

# COMMAND ----------

# MAGIC %md
# MAGIC ####Referential integrity, all 8 relationships, one mechanism (anti-join)
# MAGIC No formal FK constraint exists anywhere in this project. Every query
# MAGIC follows the same two rules: filter an SCD Type 2 parent to its current row,
# MAGIC exclude NULLs on nullable (clickstream) columns.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.stepright.dq_referential_integrity AS
SELECT 'orders_to_customers' AS relationship, COUNT(*) AS orphaned_count, current_timestamp() AS checked_at
FROM {catalog}.stepright.silver_orders o
WHERE NOT EXISTS (SELECT 1 FROM {catalog}.stepright.silver_customers c WHERE c.customer_id = o.customer_id AND c.__END_AT IS NULL)

UNION ALL
SELECT 'order_items_to_orders', COUNT(*), current_timestamp()
FROM {catalog}.stepright.silver_order_items oi
WHERE NOT EXISTS (SELECT 1 FROM {catalog}.stepright.silver_orders o WHERE o.order_id = oi.order_id AND o.__END_AT IS NULL)

UNION ALL
SELECT 'order_items_to_products', COUNT(*), current_timestamp()
FROM {catalog}.stepright.silver_order_items oi
WHERE NOT EXISTS (SELECT 1 FROM {catalog}.stepright.silver_products p WHERE p.product_id = oi.product_id)

UNION ALL
SELECT 'products_to_categories', COUNT(*), current_timestamp()
FROM {catalog}.stepright.silver_products p
WHERE NOT EXISTS (SELECT 1 FROM {catalog}.stepright.silver_categories c WHERE c.category_id = p.category_id)

UNION ALL
SELECT 'inventory_to_products', COUNT(*), current_timestamp()
FROM {catalog}.stepright.silver_inventory i
WHERE NOT EXISTS (SELECT 1 FROM {catalog}.stepright.silver_products p WHERE p.product_id = i.product_id)

UNION ALL
SELECT 'clickstream_to_products', COUNT(*), current_timestamp()
FROM {catalog}.stepright.silver_clickstream ev
WHERE ev.product_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM {catalog}.stepright.silver_products p WHERE p.product_id = ev.product_id)

UNION ALL
SELECT 'clickstream_to_customers', COUNT(*), current_timestamp()
FROM {catalog}.stepright.silver_clickstream ev
WHERE ev.customer_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM {catalog}.stepright.silver_customers c WHERE c.customer_id = ev.customer_id AND c.__END_AT IS NULL)

UNION ALL
SELECT 'clickstream_to_orders', COUNT(*), current_timestamp()
FROM {catalog}.stepright.silver_clickstream ev
WHERE ev.order_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM {catalog}.stepright.silver_orders o WHERE o.order_id = ev.order_id AND o.__END_AT IS NULL)
""")

print(f"Created {catalog}.stepright.dq_referential_integrity")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.stepright.dq_referential_integrity

# COMMAND ----------

