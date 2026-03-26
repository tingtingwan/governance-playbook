-- Databricks notebook source
-- Governance Demo: Row Filtering

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Row Filtering
-- MAGIC
-- MAGIC **Problem:** UK distributors should only see UK data. EU compliance only EU. APAC only APAC.
-- MAGIC Today this means separate tables or application-level filtering.
-- MAGIC
-- MAGIC **Solution:** Row filter functions enforce jurisdiction-based access automatically.

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'retail_insight_demo_catalog';
CREATE WIDGET TEXT schema DEFAULT 'governance_demo';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Create Filter Functions

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ${catalog}.${schema}.filter_jurisdiction(jurisdiction STRING)
RETURNS BOOLEAN RETURN jurisdiction IN ('UK', 'EU');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Apply Row Filter

-- COMMAND ----------

ALTER TABLE ${catalog}.${schema}.client_documents_filtered
SET ROW FILTER ${catalog}.${schema}.filter_jurisdiction ON (jurisdiction);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: Compare

-- COMMAND ----------

SELECT 'ORIGINAL — All Regions' as view, doc_id, doc_type, client_name, jurisdiction, compliance_status, extracted_value
FROM ${catalog}.${schema}.client_documents ORDER BY doc_id;

-- COMMAND ----------

SELECT 'FILTERED — UK & EU Only' as view, doc_id, doc_type, client_name, jurisdiction, compliance_status, extracted_value
FROM ${catalog}.${schema}.client_documents_filtered ORDER BY doc_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Key Points
-- MAGIC
-- MAGIC - 10 rows → 6 rows (APAC, US, LATAM hidden)
-- MAGIC - **Same query, same table** — filter is invisible to the user
-- MAGIC - Agents and Genie query the table directly — ABAC enforces the filter
-- MAGIC - In production: filter function looks up `current_user()` in a mapping table
