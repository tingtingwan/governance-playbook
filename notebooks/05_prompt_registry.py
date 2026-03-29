# Databricks notebook source
# Governance Demo: Prompt Registry
# Shows how to version, manage, and deploy extraction prompts via MLflow Prompt Registry

# COMMAND ----------

# MAGIC %md
# MAGIC # Prompt Registry for Document Extraction
# MAGIC
# MAGIC **The ask:** Business users want to iterate on extraction prompts
# MAGIC without a full sprint cycle. The Prompt Registry gives them:
# MAGIC
# MAGIC 1. **Version control** for prompts (like Git for prompts)
# MAGIC 2. **Aliases** (`production`, `staging`, `dev`) to safely swap prompts
# MAGIC 3. **No code changes needed** — app always loads `@production`, you just move the alias
# MAGIC 4. **Audit trail** — who changed what prompt, when, and why

# COMMAND ----------

# MAGIC %pip install mlflow>=3.1 requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_insight_demo_catalog")
dbutils.widgets.text("schema", "governance_demo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

import mlflow
import json
import requests

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")

PROMPT_NAME = f"{catalog}.{schema}.fund_doc_extractor"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Register Prompt v1 — Basic Extraction

# COMMAND ----------

v1 = mlflow.genai.register_prompt(
    name=PROMPT_NAME,
    template="""Extract the following fields from the fund document text below.
Return a JSON object with these keys: fund_name, isin, risk_rating, ongoing_charges, doc_date.
If a field is not found, use null.

Document text:
{{doc_text}}""",
    commit_message="v1: Basic extraction - simple field list",
    tags={
        "author": "tingting.wan",
        "use_case": "fund_document_extraction",
        "model": "claude-sonnet-4"
    }
)
print(f"Registered: {v1.name} v{v1.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Register Prompt v2 — Detailed with Format Hints

# COMMAND ----------

v2 = mlflow.genai.register_prompt(
    name=PROMPT_NAME,
    template="""You are a fund data extraction specialist.
Extract structured data from the following parsed fund document.

Required fields:
- fund_name: The full official fund name
- isin: The ISIN code (format: 2 letters + 10 alphanumeric characters)
- risk_rating: The risk indicator as a number 1-7. If shown as "X out of 7", return just X
- ongoing_charges: The ongoing charges / TER as a percentage string (e.g. "1.45%")
- doc_date: Publication date in ISO format (YYYY-MM-DD)
- asset_class: Primary asset class (Equity, Fixed Income, Multi-Asset, ESG)
- management_company: The fund management company name

Return ONLY valid JSON. If a field cannot be determined, use null.

Document text:
{{doc_text}}""",
    commit_message="v2: Added format hints, more fields, role context",
    tags={
        "author": "tingting.wan",
        "use_case": "fund_document_extraction",
        "model": "claude-sonnet-4",
        "improvements": "format_hints, extra_fields"
    }
)
print(f"Registered: {v2.name} v{v2.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Set Aliases
# MAGIC
# MAGIC This is the key workflow: `production` points to the stable prompt,
# MAGIC `staging` to the candidate. Business users can promote without code changes.

# COMMAND ----------

# v1 goes to production (safe, tested)
mlflow.genai.set_prompt_alias(name=PROMPT_NAME, alias="production", version=v1.version)
print(f"Set 'production' -> v{v1.version}")

# v2 goes to staging (new, under evaluation)
mlflow.genai.set_prompt_alias(name=PROMPT_NAME, alias="staging", version=v2.version)
print(f"Set 'staging' -> v{v2.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Load & Use Prompts by Alias
# MAGIC
# MAGIC The application code NEVER changes — it always loads `@production`.
# MAGIC When you're ready, just move the alias.

# COMMAND ----------

# This is what the production app does — always loads @production
prod_prompt = mlflow.genai.load_prompt(f"prompts:/{PROMPT_NAME}@production")
print(f"Production prompt (v{prod_prompt.version}):")
print(prod_prompt.template[:200] + "...")

# COMMAND ----------

# This is what the staging/test environment does
staging_prompt = mlflow.genai.load_prompt(f"prompts:/{PROMPT_NAME}@staging")
print(f"Staging prompt (v{staging_prompt.version}):")
print(staging_prompt.template[:200] + "...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Run Both Prompts Against a Test Document

# COMMAND ----------

test_doc = """
Key Investor Information
FE Global Equity Fund
ISIN: GB00B1XFGM25
Management Company: EuroBond Capital Management Ltd

Risk Indicator: 5 out of 7
Ongoing charges: 1.45%
Published: 15 January 2024
"""

# Setup LLM call
SERVING_HOST = spark.conf.get("spark.databricks.workspaceUrl")
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

def call_llm(prompt_text):
    resp = requests.post(
        f"https://{SERVING_HOST}/serving-endpoints/databricks-claude-sonnet-4/invocations",
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
        json={"messages": [
            {"role": "system", "content": "You are a precise data extraction assistant. Return only valid JSON."},
            {"role": "user", "content": prompt_text}
        ], "max_tokens": 500, "temperature": 0.0}
    )
    resp.raise_for_status()
    text = resp.json()["choices"][0]["message"]["content"]
    # Strip markdown code blocks
    if text.strip().startswith("```"):
        lines = text.strip().split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)
    return json.loads(text)

# COMMAND ----------

# Run production prompt (v1)
prod_result = call_llm(prod_prompt.format(doc_text=test_doc))
print("Production (v1) result:")
print(json.dumps(prod_result, indent=2))

# COMMAND ----------

# Run staging prompt (v2)
staging_result = call_llm(staging_prompt.format(doc_text=test_doc))
print("Staging (v2) result:")
print(json.dumps(staging_result, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Compare Results

# COMMAND ----------

import pandas as pd

comparison = pd.DataFrame({
    "Field": list(set(list(prod_result.keys()) + list(staging_result.keys()))),
})
comparison["Production (v1)"] = comparison["Field"].map(lambda f: str(prod_result.get(f, "N/A")))
comparison["Staging (v2)"] = comparison["Field"].map(lambda f: str(staging_result.get(f, "N/A")))
display(comparison)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Promote Staging to Production
# MAGIC
# MAGIC v2 looks better — more fields, cleaner format. Let's promote it.

# COMMAND ----------

# Save current production as rollback
mlflow.genai.set_prompt_alias(name=PROMPT_NAME, alias="production_previous", version=v1.version)
print(f"Saved v{v1.version} as 'production_previous' (rollback)")

# Promote v2 to production
mlflow.genai.set_prompt_alias(name=PROMPT_NAME, alias="production", version=v2.version)
print(f"Promoted v{v2.version} to 'production'")

# Verify
new_prod = mlflow.genai.load_prompt(f"prompts:/{PROMPT_NAME}@production")
print(f"\nProduction is now v{new_prod.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: List All Prompt Versions

# COMMAND ----------

prompts = mlflow.genai.search_prompts(f"catalog = '{catalog}' AND schema = '{schema}'")
for p in prompts:
    print(f"Prompt: {p.name}")
    # Load each version individually
    for ver_num in range(1, 3):
        try:
            v = mlflow.genai.load_prompt(f"prompts:/{p.name}/{ver_num}")
            print(f"  v{ver_num}: {v.template[:80]}...")
        except Exception:
            pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## What This Means for Your Organisation
# MAGIC
# MAGIC | Today | With Prompt Registry |
# MAGIC |-------|---------------------|
# MAGIC | Prompt changes = code deployment | Prompt changes = alias swap |
# MAGIC | 2-week sprint per prompt change | 30 minutes to test & promote |
# MAGIC | No audit trail on prompt versions | Full version history with tags |
# MAGIC | Business users can't self-serve | Business users test in staging, SA promotes |
# MAGIC | Rollback = redeploy old code | Rollback = `set_alias("production", old_version)` |
# MAGIC
# MAGIC ### The Workflow
# MAGIC ```
# MAGIC Business User: "The date extraction is wrong for KIID docs"
# MAGIC     |
# MAGIC     v
# MAGIC Register new prompt version (v3) with fix
# MAGIC     |
# MAGIC     v
# MAGIC Set alias: staging -> v3
# MAGIC     |
# MAGIC     v
# MAGIC Test against sample docs (compare v2 vs v3)
# MAGIC     |
# MAGIC     v
# MAGIC Looks good? Promote: production -> v3
# MAGIC     |
# MAGIC     v
# MAGIC Problem? Rollback: production -> v2
# MAGIC ```
# MAGIC
# MAGIC **Zero code changes. Zero deployments. Full audit trail.**
