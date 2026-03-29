# Databricks notebook source
# Phase 2 (Pilot): Quality Gate — Automated Evaluation Before Prompt Promotion
# This notebook runs as a Databricks Job task after extraction, blocking promotion if quality drops.

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality Gate: Evaluate Extraction Quality
# MAGIC
# MAGIC **This notebook is designed to run as a task in a Databricks Workflow.**
# MAGIC
# MAGIC It evaluates extraction quality using MLflow LLM Judges, then either:
# MAGIC - **PASSES** → sets a task value allowing the next task to promote the prompt
# MAGIC - **FAILS** → raises an exception, blocking promotion and alerting the team
# MAGIC
# MAGIC ```
# MAGIC Workflow:
# MAGIC   [Parse Docs] → [Extract] → [THIS: Evaluate] → [Promote or Block]
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install "mlflow[databricks]>=3.5"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_insight_demo_catalog")
dbutils.widgets.text("schema", "governance_demo")
dbutils.widgets.text("correctness_threshold", "0.7")
dbutils.widgets.text("guidelines_threshold", "0.8")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
correctness_threshold = float(dbutils.widgets.get("correctness_threshold"))
guidelines_threshold = float(dbutils.widgets.get("guidelines_threshold"))

# COMMAND ----------

import mlflow
import json

mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(f"/Users/{spark.sql('SELECT current_user()').first()[0]}/governance_quality_gate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Recent Extraction Results
# MAGIC
# MAGIC Pull the latest batch of extractions from the Gold table.
# MAGIC In production, this would be scoped to the current pipeline run.

# COMMAND ----------

recent_extractions = spark.sql(f"""
    SELECT file_path, full_text, doc_type, fund_name, isin, risk_rating,
           ongoing_charges, doc_date, client_name, jurisdiction, compliance_status
    FROM {catalog}.{schema}.gold_extracted_entities
    WHERE extracted_at > current_timestamp() - INTERVAL 24 HOURS
    ORDER BY extracted_at DESC
    LIMIT 100
""").collect()

print(f"Evaluating {len(recent_extractions)} recent extractions")

if len(recent_extractions) == 0:
    print("No recent extractions found. Skipping evaluation.")
    dbutils.jobs.taskValues.set(key="quality_gate_passed", value=True)
    dbutils.jobs.taskValues.set(key="reason", value="no_data")
    dbutils.notebook.exit("SKIP: No recent extractions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build Evaluation Dataset
# MAGIC
# MAGIC MLflow evaluate expects `inputs`, `outputs`, and optionally `expectations`.
# MAGIC For extractions, the output is the structured JSON and expectations
# MAGIC are guidelines about format and completeness.

# COMMAND ----------

eval_data = []
for row in recent_extractions:
    extracted = {
        "doc_type": row.doc_type,
        "fund_name": row.fund_name,
        "isin": row.isin,
        "risk_rating": str(row.risk_rating) if row.risk_rating else None,
        "ongoing_charges": row.ongoing_charges,
        "doc_date": row.doc_date,
        "client_name": row.client_name,
        "jurisdiction": row.jurisdiction,
        "compliance_status": row.compliance_status,
    }
    eval_data.append({
        "inputs": {"query": f"Extract structured data from this document: {row.full_text[:500]}..."},
        "outputs": {"response": json.dumps(extracted)},
        "expectations": {
            "expected_facts": [
                f"doc_type should be one of: KIID, Fund Factsheet, Trade Confirmation, Compliance Report, Other",
                f"If ISIN is present in the text, it should be extracted",
                f"Dates should be in YYYY-MM-DD format",
                f"jurisdiction should be a valid country or region",
            ]
        }
    })

print(f"Built {len(eval_data)} evaluation records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Run LLM Judge Evaluation

# COMMAND ----------

from mlflow.genai.scorers import Correctness, Guidelines

eval_results = mlflow.genai.evaluate(
    data=eval_data,
    scorers=[
        Correctness(),
        Guidelines(
            name="extraction_quality",
            guidelines=[
                "The response must be valid JSON",
                "All extracted values must come from the source document, not hallucinated",
                "Dates should be in YYYY-MM-DD format",
                "Missing fields should be null, not omitted or empty string",
                "ISIN codes must match the pattern: 2 letters followed by 10 alphanumeric characters",
                "Risk ratings should be a number 1-7, not '5 out of 7'",
            ]
        )
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Quality Gate Decision

# COMMAND ----------

results_df = eval_results.tables["eval_results"]

# Compute aggregate scores
# MLflow 3.5+ uses /value columns with yes/no strings
correctness_col = "correctness/score" if "correctness/score" in results_df.columns else "correctness/value"
guidelines_col = "extraction_quality/score" if "extraction_quality/score" in results_df.columns else "extraction_quality/value"

if correctness_col.endswith("/value"):
    # Convert yes/no to numeric
    avg_correctness = (results_df[correctness_col] == "yes").mean()
    avg_guidelines = (results_df[guidelines_col] == "yes").mean()
else:
    avg_correctness = results_df[correctness_col].mean()
    avg_guidelines = results_df[guidelines_col].mean()

print(f"Average Correctness Score: {avg_correctness:.3f} (threshold: {correctness_threshold})")
print(f"Average Guidelines Score:  {avg_guidelines:.3f} (threshold: {guidelines_threshold})")

correctness_pass = avg_correctness >= correctness_threshold
guidelines_pass = avg_guidelines >= guidelines_threshold
gate_passed = correctness_pass and guidelines_pass

print(f"\nQuality Gate: {'PASSED' if gate_passed else 'FAILED'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Save Results + Set Task Values

# COMMAND ----------

# Save evaluation summary to Delta for audit trail
import pandas as pd

summary = pd.DataFrame([{
    "eval_timestamp": pd.Timestamp.now(),
    "num_records": len(eval_data),
    "avg_correctness": float(avg_correctness),
    "avg_guidelines": float(avg_guidelines),
    "correctness_threshold": correctness_threshold,
    "guidelines_threshold": guidelines_threshold,
    "gate_passed": gate_passed,
}])

spark.createDataFrame(summary).write.mode("append").saveAsTable(
    f"{catalog}.{schema}.quality_gate_history"
)

# Set task values for downstream Workflow tasks
dbutils.jobs.taskValues.set(key="quality_gate_passed", value=gate_passed)
dbutils.jobs.taskValues.set(key="avg_correctness", value=float(avg_correctness))
dbutils.jobs.taskValues.set(key="avg_guidelines", value=float(avg_guidelines))

if not gate_passed:
    failures = []
    if not correctness_pass:
        failures.append(f"Correctness {avg_correctness:.3f} < {correctness_threshold}")
    if not guidelines_pass:
        failures.append(f"Guidelines {avg_guidelines:.3f} < {guidelines_threshold}")
    reason = "; ".join(failures)
    dbutils.jobs.taskValues.set(key="reason", value=reason)
    raise Exception(f"QUALITY GATE FAILED: {reason}")

print("Quality gate passed. Prompt promotion can proceed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## How This Fits in a Databricks Workflow
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
# MAGIC │  Task 1: Run     │────▶│  Task 2: Quality  │────▶│  Task 3: Promote │
# MAGIC │  DLT Pipeline    │     │  Gate (this nb)   │     │  Prompt (if pass)│
# MAGIC └──────────────────┘     └──────────────────┘     └──────────────────┘
# MAGIC                                   │
# MAGIC                                   │ FAIL
# MAGIC                                   ▼
# MAGIC                          ┌──────────────────┐
# MAGIC                          │  Task 3b: Alert  │
# MAGIC                          │  (block + notify)│
# MAGIC                          └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Workflow condition on Task 3:**
# MAGIC ```
# MAGIC {{tasks.quality_gate.values.quality_gate_passed}} == true
# MAGIC ```
