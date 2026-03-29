# Databricks notebook source
# Phase 3 (Production): Prompt CI/CD — Automated Evaluation + Promotion
# Run as a Databricks Job triggered when a new prompt version is registered.

# COMMAND ----------

# MAGIC %md
# MAGIC # Prompt CI/CD: Evaluate → Gate → Promote
# MAGIC
# MAGIC **Production workflow for prompt changes in a regulated environment.**
# MAGIC
# MAGIC When a business user or data scientist registers a new prompt version,
# MAGIC this job automatically:
# MAGIC 1. Loads the new prompt (latest version, aliased as `staging`)
# MAGIC 2. Runs it against a test corpus
# MAGIC 3. Evaluates with LLM judges
# MAGIC 4. Compares against the current production prompt
# MAGIC 5. Promotes if quality improves or meets threshold — blocks otherwise
# MAGIC
# MAGIC **Databricks resources used:**
# MAGIC - MLflow Prompt Registry (versioning + aliases)
# MAGIC - Foundation Model API (extraction)
# MAGIC - MLflow genai.evaluate() (LLM judges)
# MAGIC - Databricks Workflow (orchestration + conditional tasks)
# MAGIC - Delta table (audit trail)

# COMMAND ----------

# MAGIC %pip install "mlflow[databricks]>=3.5" requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_insight_demo_catalog")
dbutils.widgets.text("schema", "governance_demo")
dbutils.widgets.text("prompt_name", "")
dbutils.widgets.text("min_correctness", "0.7")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
prompt_name = dbutils.widgets.get("prompt_name") or f"{catalog}.{schema}.doc_extractor"
min_correctness = float(dbutils.widgets.get("min_correctness"))

# COMMAND ----------

import mlflow
import json
import requests

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Users/{spark.sql('SELECT current_user()').first()[0]}/prompt_cicd")

SERVING_HOST = spark.conf.get("spark.databricks.workspaceUrl")
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Production and Staging Prompts

# COMMAND ----------

def load_prompt_safe(alias):
    try:
        p = mlflow.genai.load_prompt(f"prompts:/{prompt_name}@{alias}")
        return {"version": p.version, "template": p.template, "alias": alias}
    except Exception as e:
        return {"error": str(e), "alias": alias}

prod_prompt = load_prompt_safe("production")
staging_prompt = load_prompt_safe("staging")

if "error" in staging_prompt:
    dbutils.notebook.exit(f"SKIP: No staging prompt found — {staging_prompt['error']}")

print(f"Production: v{prod_prompt.get('version', 'NONE')}")
print(f"Staging:    v{staging_prompt['version']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Test Corpus
# MAGIC
# MAGIC A curated set of documents with known expected outputs,
# MAGIC stored in a Delta table. This is the ground truth for evaluation.

# COMMAND ----------

test_corpus = spark.sql(f"""
    SELECT doc_id, doc_type, parsed_text, expected_facts
    FROM {catalog}.{schema}.eval_test_corpus
    ORDER BY doc_id
""").collect()

if len(test_corpus) == 0:
    print("WARN: No test corpus found. Using sample docs from notebook 04 pattern.")
    # Fallback to inline test data (same as demo notebooks)
    test_corpus_data = [
        {"doc_id": "KIID-001", "parsed_text": "Key Investor Information Document (KIID)\nGlobal Equity Growth Fund\nISIN: LU0292096186\nRisk Indicator: 5 out of 7\nOngoing charges: 1.45%\nDate of Publication: 15 January 2024",
         "expected_facts": ["Fund name is Global Equity Growth Fund", "ISIN is LU0292096186", "Risk rating is 5", "Ongoing charges are 1.45%"]},
        {"doc_id": "TRADE-001", "parsed_text": "TRADE CONFIRMATION\nTrade Reference: TC-2024-FX-00847291\nClient Name: David Park\nTransaction Type: FX Forward\nNotional Amount: EUR 500,000.00\nJurisdiction: United States",
         "expected_facts": ["Trade reference is TC-2024-FX-00847291", "Client name is David Park", "Transaction type is FX Forward", "Jurisdiction is United States"]},
    ]
else:
    test_corpus_data = [{"doc_id": r.doc_id, "parsed_text": r.parsed_text, "expected_facts": json.loads(r.expected_facts)} for r in test_corpus]

print(f"Test corpus: {len(test_corpus_data)} documents")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Run Extraction with Both Prompts

# COMMAND ----------

def call_llm(prompt_text):
    resp = requests.post(
        f"https://{SERVING_HOST}/serving-endpoints/databricks-claude-sonnet-4/invocations",
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
        json={"messages": [
            {"role": "system", "content": "You are a precise data extraction assistant. Return only valid JSON."},
            {"role": "user", "content": prompt_text}
        ], "max_tokens": 1000, "temperature": 0.0}
    )
    resp.raise_for_status()
    text = resp.json()["choices"][0]["message"]["content"]
    if text.strip().startswith("```"):
        lines = text.strip().split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)
    return text

def run_extraction(prompt_info, test_data):
    results = []
    for doc in test_data:
        filled = prompt_info["template"].replace("{{doc_text}}", doc["parsed_text"])
        raw_output = call_llm(filled)
        results.append({
            "doc_id": doc["doc_id"],
            "response": raw_output,
            "expected_facts": doc["expected_facts"],
        })
    return results

staging_results = run_extraction(staging_prompt, test_corpus_data)
print(f"Staging extraction complete: {len(staging_results)} docs")

prod_results = None
if "error" not in prod_prompt:
    prod_results = run_extraction(prod_prompt, test_corpus_data)
    print(f"Production extraction complete: {len(prod_results)} docs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Evaluate with LLM Judges

# COMMAND ----------

from mlflow.genai.scorers import Correctness, Guidelines

SCORERS = [
    Correctness(),
    Guidelines(
        name="extraction_quality",
        guidelines=[
            "The response must be valid JSON",
            "All extracted values must come from the source document, not hallucinated",
            "Dates should be in YYYY-MM-DD format",
            "Missing fields should be null, not omitted",
        ]
    )
]

def evaluate_results(results, run_name):
    eval_data = [{
        "inputs": {"query": f"Extract structured data from document {r['doc_id']}"},
        "outputs": {"response": r["response"]},
        "expectations": {"expected_facts": r["expected_facts"]}
    } for r in results]

    with mlflow.start_run(run_name=run_name):
        return mlflow.genai.evaluate(data=eval_data, scorers=SCORERS)

def get_score(eval_result, scorer_name):
    """Extract score from evaluation results, handling both /score and /value column formats."""
    df = eval_result.tables["eval_results"]
    score_col = f"{scorer_name}/score"
    value_col = f"{scorer_name}/value"
    if score_col in df.columns:
        return df[score_col].mean()
    elif value_col in df.columns:
        return (df[value_col] == "yes").mean()
    return 0.0

staging_eval = evaluate_results(staging_results, f"staging_v{staging_prompt['version']}")
staging_correctness = get_score(staging_eval, "correctness")
staging_guidelines = get_score(staging_eval, "extraction_quality")

print(f"Staging v{staging_prompt['version']}: correctness={staging_correctness:.3f}, guidelines={staging_guidelines:.3f}")

prod_correctness = None
if prod_results:
    prod_eval = evaluate_results(prod_results, f"production_v{prod_prompt['version']}")
    prod_correctness = get_score(prod_eval, "correctness")
    prod_guidelines = get_score(prod_eval, "extraction_quality")
    print(f"Production v{prod_prompt['version']}: correctness={prod_correctness:.3f}, guidelines={prod_guidelines:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Promotion Decision

# COMMAND ----------

promote = False
reason = ""

if staging_correctness >= min_correctness:
    if prod_correctness is None:
        promote = True
        reason = f"No existing production prompt. Staging v{staging_prompt['version']} meets threshold ({staging_correctness:.3f} >= {min_correctness})"
    elif staging_correctness >= prod_correctness:
        promote = True
        reason = f"Staging v{staging_prompt['version']} ({staging_correctness:.3f}) >= Production v{prod_prompt['version']} ({prod_correctness:.3f})"
    else:
        reason = f"Staging v{staging_prompt['version']} ({staging_correctness:.3f}) < Production v{prod_prompt['version']} ({prod_correctness:.3f}). Regression detected."
else:
    reason = f"Staging v{staging_prompt['version']} correctness ({staging_correctness:.3f}) below threshold ({min_correctness})"

print(f"Decision: {'PROMOTE' if promote else 'BLOCK'}")
print(f"Reason: {reason}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Execute Promotion or Block

# COMMAND ----------

import pandas as pd

if promote:
    # Save rollback alias
    if "error" not in prod_prompt:
        mlflow.genai.set_prompt_alias(
            name=prompt_name, alias="production_previous", version=prod_prompt["version"]
        )
        print(f"Saved rollback: production_previous → v{prod_prompt['version']}")

    # Promote staging to production
    mlflow.genai.set_prompt_alias(
        name=prompt_name, alias="production", version=staging_prompt["version"]
    )
    print(f"PROMOTED: production → v{staging_prompt['version']}")
else:
    print(f"BLOCKED: Staging v{staging_prompt['version']} not promoted. {reason}")

# Audit trail
audit_record = pd.DataFrame([{
    "timestamp": pd.Timestamp.now(),
    "prompt_name": prompt_name,
    "staging_version": int(staging_prompt["version"]),
    "production_version": int(prod_prompt["version"]) if "error" not in prod_prompt else None,
    "staging_correctness": float(staging_correctness),
    "staging_guidelines": float(staging_guidelines),
    "production_correctness": float(prod_correctness) if prod_correctness else None,
    "decision": "PROMOTE" if promote else "BLOCK",
    "reason": reason,
}])

spark.createDataFrame(audit_record).write.mode("append").saveAsTable(
    f"{catalog}.{schema}.prompt_cicd_audit"
)

# Task values for Workflow conditional logic
dbutils.jobs.taskValues.set(key="promoted", value=promote)
dbutils.jobs.taskValues.set(key="reason", value=reason)

if not promote:
    raise Exception(f"PROMPT CI/CD BLOCKED: {reason}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deployment as a Databricks Workflow
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "prompt-cicd-doc-extractor",
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC       "task_key": "evaluate_and_promote",
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/path/to/production/01_prompt_cicd",
# MAGIC         "base_parameters": {
# MAGIC           "catalog": "retail_insight_demo_catalog",
# MAGIC           "schema": "governance_demo",
# MAGIC           "min_correctness": "0.7"
# MAGIC         }
# MAGIC       }
# MAGIC     }
# MAGIC   ],
# MAGIC   "trigger": {
# MAGIC     "pause_status": "UNPAUSED"
# MAGIC   },
# MAGIC   "email_notifications": {
# MAGIC     "on_failure": ["tingting.wan@databricks.com"]
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Trigger options:**
# MAGIC - Scheduled (e.g., daily after pipeline run)
# MAGIC - Manual (when data scientist registers a new prompt)
# MAGIC - Event-driven (future: webhook on prompt version creation)
