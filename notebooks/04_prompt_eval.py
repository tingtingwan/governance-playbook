# Databricks notebook source
# Governance Demo: Prompt Evaluation with LLM Judges
# Uses MLflow Agent Evaluation to automatically score extraction quality

# COMMAND ----------

# MAGIC %md
# MAGIC # Prompt Evaluation with LLM Judges
# MAGIC
# MAGIC **Problem:** How do you know which prompt version extracts data better?
# MAGIC Manual review doesn't scale when you have 1,000+ documents per day.
# MAGIC
# MAGIC **Solution:** MLflow's built-in LLM judges automatically score extraction
# MAGIC quality — correctness, completeness, and guideline adherence.
# MAGIC No human review needed. Results visible in the MLflow UI.

# COMMAND ----------

# MAGIC %pip install "mlflow[databricks]>=3.1" requests
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
mlflow.set_experiment(f"/Users/{spark.sql('SELECT current_user()').first()[0]}/governance_prompt_eval")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Sample Documents (parsed text)

# COMMAND ----------

parsed_documents = [
    {
        "doc_id": "KIID-001",
        "doc_type": "KIID",
        "parsed_text": """Key Investor Information Document (KIID)

Global Equity Growth Fund
A sub-fund of European Investment SICAV
ISIN: LU0292096186
Management Company: FundCo Asset Management S.A.

Risk Indicator: 5 out of 7
Ongoing charges: 1.45% per annum
Performance fee: None
Entry charge: 5.00%

Past Performance: 2023: +14.2%  2022: -8.7%  2021: +22.1%

Depositary: Northern Trust Luxembourg S.A.
Date of Publication: 15 January 2024""",
        "expected_facts": [
            "Fund name is Global Equity Growth Fund",
            "ISIN is LU0292096186",
            "Risk rating is 5",
            "Ongoing charges are 1.45%",
            "Publication date is 2024-01-15 or 15 January 2024",
            "Management company is FundCo Asset Management S.A."
        ]
    },
    {
        "doc_id": "FACTSHEET-001",
        "doc_type": "Fund Factsheet",
        "parsed_text": """Fund Factsheet - Euro Corporate Bond Fund
As at 29 February 2024

ISIN: IE00BK5BQT80
Fund Manager: EuroBond Capital Management Ltd
Fund Size: EUR 2.4 billion
Risk Rating: 3 out of 7
Ongoing Charges (OCF): 0.85%
SFDR Classification: Article 8
Launch Date: 15 March 2018

Performance: Fund YTD +1.2%, 1Y +4.8%, 3Y -2.1%""",
        "expected_facts": [
            "Fund name is Euro Corporate Bond Fund",
            "ISIN is IE00BK5BQT80",
            "Risk rating is 3",
            "Ongoing charges are 0.85%",
            "SFDR classification is Article 8",
            "Fund manager is EuroBond Capital Management Ltd"
        ]
    },
    {
        "doc_id": "TRADE-001",
        "doc_type": "Trade Confirmation",
        "parsed_text": """TRADE CONFIRMATION - CONFIDENTIAL

Trade Reference: TC-2024-FX-00847291
Trade Date: 05 March 2024
Settlement Date: 07 March 2024
Transaction Type: FX Forward
Direction: BUY

Client Name: David Park
Client Email: d.park@us-advisory.com
Jurisdiction: United States

Instrument: EUR/USD FX Forward 3M
Notional Amount: EUR 500,000.00
Forward Rate: 1.0842

MiFID II Classification: Professional Client
Compliance Status: Approved""",
        "expected_facts": [
            "Trade reference is TC-2024-FX-00847291",
            "Trade date is 2024-03-05 or 05 March 2024",
            "Transaction type is FX Forward",
            "Client name is David Park",
            "Notional amount is EUR 500,000",
            "Jurisdiction is United States",
            "Compliance status is Approved"
        ]
    }
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define Prompt Versions

# COMMAND ----------

prompt_versions = {
    "v1_basic": """Extract fields from this document. Return JSON with keys: fund_name, isin, risk_rating, ongoing_charges, doc_date.
If a field is not found, use null.

Document:
{{doc_text}}""",

    "v2_detailed": """You are a financial document extraction specialist.
Extract structured data from the following document.

Required fields:
- fund_name: Full official name
- isin: ISIN code (2 letters + 10 alphanumeric)
- risk_rating: Numeric risk indicator (1-7), just the number
- ongoing_charges: As percentage string (e.g. "1.45%")
- doc_date: Publication/trade date as YYYY-MM-DD
- asset_class: Equity, Fixed Income, Multi-Asset, ESG, FX, or Other
- management_company: Fund manager or counterparty name
- compliance_status: If mentioned

Return ONLY valid JSON. Use null for missing fields.

Document:
{{doc_text}}"""
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Run Extraction

# COMMAND ----------

SERVING_HOST = spark.conf.get("spark.databricks.workspaceUrl")
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

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

# COMMAND ----------

all_results = []
for prompt_name, template in prompt_versions.items():
    for doc in parsed_documents:
        filled = template.replace("{{doc_text}}", doc["parsed_text"])
        raw_output = call_llm(filled)
        try:
            parsed = json.loads(raw_output)
            response_text = json.dumps(parsed, indent=2)
        except json.JSONDecodeError:
            response_text = raw_output

        all_results.append({
            "doc_id": doc["doc_id"],
            "doc_type": doc["doc_type"],
            "prompt_version": prompt_name,
            "response": response_text,
            "expected_facts": doc["expected_facts"]
        })

print(f"Completed {len(all_results)} extractions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Evaluate with LLM Judges
# MAGIC
# MAGIC Using MLflow's built-in **Correctness** and **Guidelines** judges
# MAGIC to automatically score extraction quality. No human review needed.

# COMMAND ----------

from mlflow.genai.scorers import Correctness, Guidelines

eval_data = []
for r in all_results:
    eval_data.append({
        "inputs": {"query": f"Extract structured data from this {r['doc_type']} document (prompt: {r['prompt_version']})"},
        "outputs": {"response": r["response"]},
        "expectations": {"expected_facts": r["expected_facts"]}
    })

# COMMAND ----------

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
                "Missing fields should be null, not omitted",
                "Risk ratings should be just the number (e.g. 5), not '5 out of 7'"
            ]
        )
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: View Results
# MAGIC
# MAGIC Click the **MLflow Experiment** link above to see:
# MAGIC - Per-example correctness scores with **judge reasoning**
# MAGIC - Guideline adherence breakdown
# MAGIC - Aggregate metrics across all documents
# MAGIC
# MAGIC The judges explain WHY they scored each extraction the way they did.

# COMMAND ----------

display(eval_results.tables["eval_results"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Save Extraction Results to Delta

# COMMAND ----------

import pandas as pd

summary_rows = []
for r in all_results:
    try:
        parsed = json.loads(r["response"])
        extracted = parsed if isinstance(parsed, dict) else {}
    except:
        extracted = {}
    summary_rows.append({
        "doc_id": str(r["doc_id"]),
        "doc_type": str(r["doc_type"]),
        "prompt": str(r["prompt_version"]),
        "fund_name": str(extracted.get("fund_name", "N/A")),
        "isin": str(extracted.get("isin", "N/A")),
        "risk_rating": str(extracted.get("risk_rating", "N/A")),
        "ongoing_charges": str(extracted.get("ongoing_charges", "N/A")),
    })

spark_df = spark.createDataFrame(pd.DataFrame(summary_rows))
spark_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.prompt_eval_results")
print(f"Saved to {catalog}.{schema}.prompt_eval_results")

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.prompt_eval_results"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What This Gives You
# MAGIC
# MAGIC | Manual Evaluation | LLM Judge Evaluation |
# MAGIC |------------------|---------------------|
# MAGIC | Human reviews each extraction | LLM judges score automatically |
# MAGIC | Hours for 100 documents | Minutes for 1,000+ documents |
# MAGIC | Subjective, inconsistent | Consistent, explainable scores |
# MAGIC | No audit trail | Full trace in MLflow with judge reasoning |
# MAGIC
# MAGIC **In production:** Run evaluation on every prompt change before promoting.
# MAGIC If correctness drops below threshold, block the promotion automatically.
