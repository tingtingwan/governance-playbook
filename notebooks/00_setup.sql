-- Databricks notebook source
-- Regulated Industry Governance Demo: Setup
-- Reusable for any FinServ customer (fund managers, trading firms, insurers)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Governance Demo: Setup
-- MAGIC
-- MAGIC Creates sample **financial services data** to demonstrate
-- MAGIC Attribute-Based Access Control (ABAC) with Unity Catalog.
-- MAGIC
-- MAGIC **Scenario:** A regulated financial institution processes client documents
-- MAGIC across regions. Different teams need different access levels:
-- MAGIC - **Compliance** sees everything
-- MAGIC - **Analysts** see masked PII
-- MAGIC - **Regional teams** only see their jurisdiction

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'retail_insight_demo_catalog';
CREATE WIDGET TEXT schema DEFAULT 'governance_demo';
CREATE WIDGET TEXT tag_key DEFAULT 'governance_pii';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema};

-- COMMAND ----------

DROP TABLE IF EXISTS ${catalog}.${schema}.client_documents;
DROP TABLE IF EXISTS ${catalog}.${schema}.client_documents_masked;
DROP TABLE IF EXISTS ${catalog}.${schema}.client_documents_filtered;
DROP TABLE IF EXISTS ${catalog}.${schema}.client_documents_governed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Sample Data: Client Documents
-- MAGIC Mix of KYC, trade confirmations, compliance reports, fund factsheets — typical regulated FinServ data.

-- COMMAND ----------

CREATE TABLE ${catalog}.${schema}.client_documents (
  doc_id STRING, doc_type STRING, client_name STRING,
  client_email STRING, client_phone STRING, jurisdiction STRING,
  instrument_type STRING, reference_number STRING,
  sensitivity STRING, compliance_status STRING,
  extracted_value DECIMAL(15,2), extraction_date DATE, confidence_score DECIMAL(5,2)
);

-- COMMAND ----------

INSERT INTO ${catalog}.${schema}.client_documents VALUES
('DOC-001', 'KYC',                'Alice Chen',        'alice.chen@globalfunds.com',  '+44-20-7946-0958', 'UK',    'Fund Prospectus',  'KYC-GB-2024-001', 'Confidential', 'Compliant',    125000.00, '2024-01-15', 0.95),
('DOC-002', 'Trade Confirmation', 'Max Zwiessele',     'max.z@tradeco.de',            '+49-30-555-1234',  'EU',    'CFD',              'TC-EU-2024-002',  'Restricted',   'Under Review', 250000.00, '2024-02-01', 0.88),
('DOC-003', 'Compliance Report',  'Kenji Tanaka',      'k.tanaka@apac-invest.jp',     '+81-3-5555-1234',  'APAC',  'Equity Swap',      'CR-AP-2024-003',  'Confidential', 'Compliant',    180000.00, '2024-01-20', 0.92),
('DOC-004', 'Fund Factsheet',     'Sarah Thompson',    's.thompson@wealth.co.uk',     '+44-20-8123-4567', 'UK',    'UCITS Fund',       'FF-GB-2024-004',  'Internal',     'Compliant',    500000.00, '2024-03-01', 0.97),
('DOC-005', 'Risk Assessment',    'Carlos Rodriguez',  'c.rodriguez@latam-fin.com',   '+55-11-9876-5432', 'LATAM', 'Structured Note',  'RA-LT-2024-005',  'Restricted',   'Flagged',       95000.00, '2024-02-15', 0.72),
('DOC-006', 'KIID',               'Emma Johansson',    'emma.j@nordic-cap.se',        '+46-8-555-0123',   'EU',    'ESG Fund',         'KI-EU-2024-006',  'Public',       'Compliant',    320000.00, '2024-01-10', 0.96),
('DOC-007', 'Trade Confirmation', 'David Park',        'd.park@us-advisory.com',      '+1-212-555-0199',  'US',    'FX Forward',       'TC-US-2024-007',  'Confidential', 'Under Review', 450000.00, '2024-03-05', 0.85),
('DOC-008', 'AML Report',         'Wei Zhang',         'w.zhang@cn-capital.cn',       '+86-10-6555-8888', 'APAC',  'Bond',             'AML-AP-2024-008', 'Restricted',   'Escalated',    175000.00, '2024-02-20', 0.68),
('DOC-009', 'Fund Prospectus',    'James Wilson',      'j.wilson@uk-pensions.com',    '+44-161-555-7890', 'UK',    'Gilt Fund',        'FP-GB-2024-009',  'Internal',     'Compliant',    680000.00, '2024-01-25', 0.94),
('DOC-010', 'Compliance Report',  'Sophie Muller',     'sophie.m@de-invest.de',       '+49-30-555-4321',  'EU',    'Multi-Asset',      'CR-EU-2024-010',  'Confidential', 'Compliant',    210000.00, '2024-03-10', 0.91);

-- COMMAND ----------

CREATE TABLE ${catalog}.${schema}.client_documents_masked AS SELECT * FROM ${catalog}.${schema}.client_documents;
CREATE TABLE ${catalog}.${schema}.client_documents_filtered AS SELECT * FROM ${catalog}.${schema}.client_documents;
CREATE TABLE ${catalog}.${schema}.client_documents_governed AS SELECT * FROM ${catalog}.${schema}.client_documents;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tag PII Columns
-- MAGIC Tags are the foundation of ABAC — tag once, enforce everywhere.

-- COMMAND ----------

ALTER TABLE ${catalog}.${schema}.client_documents_masked ALTER COLUMN client_email SET TAGS ('${tag_key}' = 'pii_email');
ALTER TABLE ${catalog}.${schema}.client_documents_masked ALTER COLUMN client_phone SET TAGS ('${tag_key}' = 'pii_phone');
ALTER TABLE ${catalog}.${schema}.client_documents_masked ALTER COLUMN reference_number SET TAGS ('${tag_key}' = 'pii_reference');

ALTER TABLE ${catalog}.${schema}.client_documents_governed ALTER COLUMN client_email SET TAGS ('${tag_key}' = 'pii_email');
ALTER TABLE ${catalog}.${schema}.client_documents_governed ALTER COLUMN client_phone SET TAGS ('${tag_key}' = 'pii_phone');
ALTER TABLE ${catalog}.${schema}.client_documents_governed ALTER COLUMN reference_number SET TAGS ('${tag_key}' = 'pii_reference');

-- COMMAND ----------

SELECT * FROM ${catalog}.${schema}.client_documents ORDER BY doc_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC ## Data Lineage
-- MAGIC Unity Catalog tracks lineage automatically — who accessed what, when, and what was derived.

-- COMMAND ----------

-- Show table lineage (column-level tracking)
-- This shows the flow: raw data → governed tables → downstream queries
DESCRIBE DETAIL ${catalog}.${schema}.client_documents;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Audit: Who Accessed Governed Data?
-- MAGIC System tables capture every access attempt — including masked/filtered queries.
-- MAGIC This is critical for regulatory compliance (MiFID II, GDPR, DORA audit requirements).

-- COMMAND ----------

-- Query audit logs for access to governance demo tables
-- NOTE: system.access.audit may take a few minutes to populate after queries run
SELECT
  event_time,
  user_identity.email as user,
  action_name,
  request_params.full_name_arg as table_accessed,
  response.status_code
FROM system.access.audit
WHERE request_params.full_name_arg LIKE '${catalog}.${schema}.%'
  AND event_time > current_timestamp() - INTERVAL 1 HOUR
ORDER BY event_time DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup Complete
-- MAGIC
-- MAGIC | Table | Purpose |
-- MAGIC |-------|---------|
-- MAGIC | `client_documents` | Baseline — all data visible |
-- MAGIC | `client_documents_masked` | Column masking demo |
-- MAGIC | `client_documents_filtered` | Row filtering demo |
-- MAGIC | `client_documents_governed` | Both masking + filtering |
-- MAGIC
-- MAGIC **Governance features demonstrated:**
-- MAGIC - Column tags for PII classification
-- MAGIC - Data lineage tracked in Unity Catalog
-- MAGIC - Access audit trail via system tables
