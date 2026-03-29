-- Databricks notebook source
-- Phase 3 (Production): Monitoring Alerts
-- SQL queries for Databricks SQL Alerts — quality drift detection + audit compliance

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Production Monitoring Alerts
-- MAGIC
-- MAGIC **These SQL queries are designed for Databricks SQL Alerts.**
-- MAGIC
-- MAGIC Each query monitors a specific production concern for regulated document processing:
-- MAGIC
-- MAGIC | Alert | What It Detects | Threshold |
-- MAGIC |-------|----------------|-----------|
-- MAGIC | Quality Drift | Extraction correctness drops | avg < 0.7 |
-- MAGIC | Parse Failures | Spike in ai_parse_document errors | > 10% failure rate |
-- MAGIC | Prompt Rollback | CI/CD blocked a promotion | Any BLOCK event |
-- MAGIC | Audit Compliance | Access to governed tables | New access patterns |
-- MAGIC
-- MAGIC **To create an alert:** Copy the query into a Databricks SQL Query,
-- MAGIC then create an Alert with the specified trigger condition.

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'retail_insight_demo_catalog';
CREATE WIDGET TEXT schema DEFAULT 'governance_demo';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Alert 1: Extraction Quality Drift
-- MAGIC
-- MAGIC **Trigger:** Average correctness score drops below threshold in last 24 hours.
-- MAGIC **Schedule:** Every 1 hour.
-- MAGIC **Action:** Email + Slack notification to SA and data science team.

-- COMMAND ----------

-- DBSQL Alert Query: Quality Drift
-- Trigger condition: avg_correctness < 0.7
SELECT
  DATE_TRUNC('hour', eval_timestamp) AS eval_hour,
  COUNT(*) AS num_evaluations,
  ROUND(AVG(avg_correctness), 3) AS avg_correctness,
  ROUND(AVG(avg_guidelines), 3) AS avg_guidelines,
  CASE
    WHEN AVG(avg_correctness) < 0.7 THEN 'ALERT: Quality below threshold'
    WHEN AVG(avg_correctness) < 0.8 THEN 'WARNING: Quality degrading'
    ELSE 'OK'
  END AS status
FROM ${catalog}.${schema}.quality_gate_history
WHERE eval_timestamp > current_timestamp() - INTERVAL 24 HOURS
GROUP BY DATE_TRUNC('hour', eval_timestamp)
ORDER BY eval_hour DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Alert 2: Document Parse Failure Rate
-- MAGIC
-- MAGIC **Trigger:** More than 10% of documents fail to parse in last 24 hours.
-- MAGIC **Schedule:** Every 30 minutes.
-- MAGIC **Action:** Page on-call engineer.

-- COMMAND ----------

-- DBSQL Alert Query: Parse Failure Rate
-- Trigger condition: failure_rate > 0.10
SELECT
  COUNT(*) AS total_docs,
  SUM(CASE WHEN parse_error IS NOT NULL THEN 1 ELSE 0 END) AS failed_docs,
  ROUND(
    SUM(CASE WHEN parse_error IS NOT NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*),
    3
  ) AS failure_rate,
  COLLECT_SET(parse_error) AS error_types
FROM ${catalog}.${schema}.parsed_documents
WHERE parsed_at > current_timestamp() - INTERVAL 24 HOURS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Alert 3: Prompt CI/CD Blocks
-- MAGIC
-- MAGIC **Trigger:** Any prompt promotion was blocked in the last 24 hours.
-- MAGIC **Schedule:** Every 1 hour.
-- MAGIC **Action:** Notify data science team lead.

-- COMMAND ----------

-- DBSQL Alert Query: CI/CD Blocks
-- Trigger condition: blocked_count > 0
SELECT
  COUNT(*) AS blocked_count,
  COLLECT_LIST(
    CONCAT('v', staging_version, ': ', reason)
  ) AS block_reasons,
  MAX(timestamp) AS latest_block
FROM ${catalog}.${schema}.prompt_cicd_audit
WHERE decision = 'BLOCK'
  AND timestamp > current_timestamp() - INTERVAL 24 HOURS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Alert 4: Governed Data Access Audit
-- MAGIC
-- MAGIC **Trigger:** New users or unusual access patterns on governed tables.
-- MAGIC **Schedule:** Daily.
-- MAGIC **Action:** Compliance officer review.
-- MAGIC
-- MAGIC > **Note:** system.access.audit has ~10-15 min typical latency (best-effort).
-- MAGIC > Retention: 365 days. Do not use for real-time alerting.

-- COMMAND ----------

-- DBSQL Alert Query: Access Audit
-- Review: new users accessing governed tables in last 24h
SELECT
  user_identity.email AS user_email,
  action_name,
  request_params.full_name_arg AS table_accessed,
  COUNT(*) AS access_count,
  MIN(event_time) AS first_access,
  MAX(event_time) AS last_access
FROM system.access.audit
WHERE request_params.full_name_arg LIKE '${catalog}.${schema}.%'
  AND event_time > current_timestamp() - INTERVAL 24 HOURS
  AND action_name IN ('commandSubmit', 'getTable', 'selectFromTable')
GROUP BY user_identity.email, action_name, request_params.full_name_arg
ORDER BY access_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Alert 5: Prompt Version History (Compliance Audit)
-- MAGIC
-- MAGIC **Not an alert** — a reporting query for regulatory review.
-- MAGIC Shows the full prompt change history with who changed what, when, and evaluation scores.

-- COMMAND ----------

-- Compliance Report: Full Prompt Change Audit Trail
SELECT
  timestamp,
  prompt_name,
  staging_version,
  production_version,
  ROUND(staging_correctness, 3) AS staging_score,
  ROUND(production_correctness, 3) AS prod_score,
  decision,
  reason
FROM ${catalog}.${schema}.prompt_cicd_audit
ORDER BY timestamp DESC
LIMIT 50;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setting Up Alerts in Databricks SQL
-- MAGIC
-- MAGIC 1. **Create a SQL Query** — paste the alert query above
-- MAGIC 2. **Create an Alert** — set trigger condition (e.g., `failure_rate > 0.10`)
-- MAGIC 3. **Set Schedule** — how often to check (1 min to custom cron)
-- MAGIC 4. **Configure Notification** — Email, Slack webhook, PagerDuty, or generic webhook
-- MAGIC
-- MAGIC **For regulated environments:**
-- MAGIC - Alert history is retained in Databricks and queryable
-- MAGIC - Combine with UC system tables for complete audit trail
-- MAGIC - Export to your SIEM/GRC tool via webhook destination
