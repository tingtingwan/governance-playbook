-- Databricks notebook source
-- Governance Demo: Combined Masking + Filtering

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Combined: Masking + Filtering
-- MAGIC
-- MAGIC One table with both row filtering AND column masking.
-- MAGIC This is the full governance picture.

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'retail_insight_demo_catalog';
CREATE WIDGET TEXT schema DEFAULT 'governance_demo';

-- COMMAND ----------

ALTER TABLE ${catalog}.${schema}.client_documents_governed
SET ROW FILTER ${catalog}.${schema}.filter_jurisdiction ON (jurisdiction);

-- COMMAND ----------

ALTER TABLE ${catalog}.${schema}.client_documents_governed ALTER COLUMN client_email SET MASK ${catalog}.${schema}.mask_email;
ALTER TABLE ${catalog}.${schema}.client_documents_governed ALTER COLUMN client_phone SET MASK ${catalog}.${schema}.mask_phone;
ALTER TABLE ${catalog}.${schema}.client_documents_governed ALTER COLUMN reference_number SET MASK ${catalog}.${schema}.mask_reference;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Side-by-Side: Compliance vs Analyst

-- COMMAND ----------

SELECT 'COMPLIANCE — Full Access' as view,
  doc_id, doc_type, client_name, client_email, client_phone,
  jurisdiction, reference_number, compliance_status, extracted_value
FROM ${catalog}.${schema}.client_documents ORDER BY doc_id;

-- COMMAND ----------

SELECT 'ANALYST — Governed Access' as view,
  doc_id, doc_type, client_name, client_email, client_phone,
  jurisdiction, reference_number, compliance_status, extracted_value
FROM ${catalog}.${schema}.client_documents_governed ORDER BY doc_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## What Changed
-- MAGIC
-- MAGIC | | Compliance View | Analyst View |
-- MAGIC |---|---|---|
-- MAGIC | **Rows** | 10 (all regions) | 6 (UK + EU only) |
-- MAGIC | **Email** | `alice.chen@globalfunds.com` | `a***@***.com` |
-- MAGIC | **Phone** | `+44-20-7946-0958` | `+44-**-****-****` |
-- MAGIC | **Reference** | `KYC-GB-2024-001` | `KYC***-****-***` |
-- MAGIC
-- MAGIC **Same table. Same query. Zero code changes.**
-- MAGIC
-- MAGIC ### How This Works with Agents
-- MAGIC ```
-- MAGIC   Supervisor Agent
-- MAGIC        |
-- MAGIC   +----+----+
-- MAGIC   |         |
-- MAGIC  Genie   Knowledge
-- MAGIC   |      Assistant
-- MAGIC   |
-- MAGIC  Unity Catalog Tables
-- MAGIC   (ABAC policies enforce
-- MAGIC    per-user access automatically)
-- MAGIC ```
