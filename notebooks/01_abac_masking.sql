-- Databricks notebook source
-- Governance Demo: Column Masking

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Column Masking
-- MAGIC
-- MAGIC **Problem:** Analysts need fund performance data but shouldn't see investor PII.
-- MAGIC Compliance needs full access. Today this means maintaining separate views per persona.
-- MAGIC
-- MAGIC **Solution:** Masking functions attached to columns. One table, automatic enforcement.

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'retail_insight_demo_catalog';
CREATE WIDGET TEXT schema DEFAULT 'governance_demo';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Create Masking Functions

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ${catalog}.${schema}.mask_email(email STRING)
RETURNS STRING RETURN CONCAT(SUBSTRING(email, 1, 1), '***@***.', SUBSTRING_INDEX(email, '.', -1));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ${catalog}.${schema}.mask_phone(phone STRING)
RETURNS STRING RETURN CONCAT(SUBSTRING(phone, 1, 4), '-**-****-****');

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ${catalog}.${schema}.mask_reference(ref STRING)
RETURNS STRING RETURN CONCAT(SUBSTRING(ref, 1, 3), '***-****-***');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Apply Masks

-- COMMAND ----------

ALTER TABLE ${catalog}.${schema}.client_documents_masked ALTER COLUMN client_email SET MASK ${catalog}.${schema}.mask_email;
ALTER TABLE ${catalog}.${schema}.client_documents_masked ALTER COLUMN client_phone SET MASK ${catalog}.${schema}.mask_phone;
ALTER TABLE ${catalog}.${schema}.client_documents_masked ALTER COLUMN reference_number SET MASK ${catalog}.${schema}.mask_reference;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: Compare

-- COMMAND ----------

SELECT 'ORIGINAL — Compliance View' as view, doc_id, doc_type, client_name, client_email, client_phone, reference_number, jurisdiction
FROM ${catalog}.${schema}.client_documents ORDER BY doc_id;

-- COMMAND ----------

SELECT 'MASKED — Analyst View' as view, doc_id, doc_type, client_name, client_email, client_phone, reference_number, jurisdiction
FROM ${catalog}.${schema}.client_documents_masked ORDER BY doc_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Why This Matters
-- MAGIC
-- MAGIC | Without Masking | With Masking |
-- MAGIC |----------------|-------------|
-- MAGIC | Separate views per persona | One table, masks enforce access |
-- MAGIC | New PII column = update all views | New PII column = add mask |
-- MAGIC | Agent queries wrong view = data leak | Agent queries table, mask is invisible |
-- MAGIC
-- MAGIC **At scale (ABAC):** Register governed tags at account level, then `CREATE POLICY` applies masking
-- MAGIC to ALL columns matching a tag across ALL tables. Zero per-column setup.
