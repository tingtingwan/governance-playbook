# Regulated Industry Governance Playbook

**From demo to deployment:** A three-phase playbook for regulated financial services companies building productionised GenAI document processing on Databricks.

## The Business Problem

Regulated organisations (fund managers, trading firms, insurers) process thousands of documents daily — KIIDs, factsheets, trade confirmations, compliance reports. They need to:

1. **Extract structured data** from these documents at scale
2. **Know the extraction is correct** — without manual review of every document
3. **Control who sees what** — PII masking, jurisdiction filtering, audit trails
4. **Iterate on prompts safely** — without a 2-week release cycle per prompt change
5. **Prove compliance** to regulators — who accessed what, when, derived from where

This playbook shows how each of these problems maps to a **specific Databricks platform capability** — not a custom app, but the platform itself.

---

## Three Phases: Demo → Pilot → Production

```
Phase 1: DEMO (prove it works)           → notebooks/
Phase 2: PILOT (run it repeatably)       → pilot/
Phase 3: PRODUCTION (harden & monitor)   → production/
```

### Phase 1: Demo — Interactive Notebooks

Run interactively to prove the concept works. 20-minute demo flow.


| Notebook                | Business Problem                             | Databricks Resource                  |
| ----------------------- | -------------------------------------------- | ------------------------------------ |
| `00_setup.sql`          | Create sample data, tag PII columns          | Unity Catalog Tags                   |
| `01_abac_masking.sql`   | Analysts need data but not PII               | UC Column Masks (GA)                 |
| `02_abac_filtering.sql` | Regional teams only see their jurisdiction   | UC Row Filters (GA)                  |
| `03_combined.sql`       | Full governance: masking + filtering         | UC ABAC                              |
| `04_prompt_eval.py`     | How do you know extraction is correct?       | MLflow LLM Judges (`genai.evaluate`) |
| `05_prompt_registry.py` | How do you update prompts without a release? | MLflow Prompt Registry (Preview)     |


**Quick Start:**

1. Upload `notebooks/` to any Databricks workspace with Unity Catalog
2. Create a serverless SQL warehouse
3. Run in order: `00` → `01` → `02` → `03`, then `04` and `05` independently
4. Set `catalog` and `schema` widgets to your target location

### Phase 2: Pilot — Scheduled Pipelines

Take the demo into a repeatable, scheduled pipeline using Databricks-native orchestration.


| Notebook                       | Business Problem                               | Databricks Resource                 |
| ------------------------------ | ---------------------------------------------- | ----------------------------------- |
| `pilot/01_dlt_doc_pipeline.py` | Process documents at scale, not one-by-one     | Lakeflow Declarative Pipeline (DLT) |
| `pilot/02_quality_gate.py`     | Block bad prompts before they reach production | MLflow Evaluate as Workflow task    |


**Key design decisions:**

- **Autoloader** (`cloudFiles` + `binaryFile`) watches a UC Volume for new PDFs
- `**ai_parse_document`** extracts text (batch-optimised — never call in a for-loop)
- `**dlt.create_sink**` persists parsed results so full refresh doesn't re-parse everything
- `**failOnError => false**` on `ai_parse_document` and `ai_query` — one bad PDF doesn't crash the pipeline
- **DLT expectations** enforce data quality at each layer (valid parse, has text, valid extraction)
- **Quality gate notebook** runs `mlflow.genai.evaluate()` and raises an exception if scores drop below threshold

### Phase 3: Production — Hardened & Monitored

Reference architecture for regulated production deployment.


| Notebook                              | Business Problem                     | Databricks Resource               |
| ------------------------------------- | ------------------------------------ | --------------------------------- |
| `production/01_prompt_cicd.py`        | Safe, auditable prompt changes       | MLflow Prompt Registry + Workflow |
| `production/02_monitoring_alerts.sql` | Detect quality drift, parse failures | Databricks SQL Alerts             |


**Production capabilities:**

- **Prompt CI/CD:** New prompt → automated eval → compare vs production → promote or block
- **Quality drift alerts:** Hourly check on extraction correctness scores
- **Parse failure alerts:** Spike detection on `ai_parse_document` error rates
- **Audit trail:** Full history of prompt changes, quality scores, access patterns
- **Rollback:** `production_previous` alias enables 30-second rollback

---

## Databricks Resource Map

Every phase maps to specific Databricks platform capabilities — this is what separates it from a generic governance playbook.


| Business Need        | Phase 1 (Demo)                | Phase 2 (Pilot)                     | Phase 3 (Production)                      |
| -------------------- | ----------------------------- | ----------------------------------- | ----------------------------------------- |
| Parse documents      | `ai_parse_document` (SQL)     | Autoloader + DLT pipeline           | Lakeflow with sinks (idempotent)          |
| Extract fields       | Foundation Model API          | `ai_query` in DLT                   | Prompt loaded from Registry `@production` |
| Version prompts      | MLflow Prompt Registry        | Same                                | CI/CD: auto-evaluate before promotion     |
| Evaluate quality     | `mlflow.genai.evaluate()`     | Quality gate Job task               | SQL Alerts on quality drift               |
| Govern access        | UC row filters + column masks | Same (applied to DLT output tables) | ABAC policies at catalog/schema level     |
| Audit trail          | `system.access.audit` query   | DLT expectations + event logs       | SQL Alerts + compliance reports           |
| Business user access | Notebook output               | Genie Space over extraction tables  | Genie + AI/BI Dashboards                  |
| Monitor              | Manual MLflow UI              | Job failure notifications           | DBSQL Alerts (quality, parse, CI/CD)      |


---

## Feature Status (March 2026)


| Feature                       | Status              | Notes                                                |
| ----------------------------- | ------------------- | ---------------------------------------------------- |
| UC Row Filters / Column Masks | **GA**              | Per-table. DBR 12.2+                                 |
| UC ABAC Policies (tag-driven) | **Public Preview**  | Catalog/schema-level inheritance. DBR 16.4+          |
| `ai_parse_document`           | **GA** (March 2026) | Batch-optimised. US + EU regions.                    |
| MLflow Prompt Registry        | **Public Preview**  | Enable from Previews page. 100K char template limit. |
| MLflow `genai.evaluate()`     | **GA** (MLflow OSS) | Managed judge models are Preview                     |
| Databricks SQL Alerts         | **Public Preview**  | Not yet in DABs                                      |
| DLT / Lakeflow Pipelines      | **GA**              | Use `create_sink` for idempotency                    |
| GEPA (Prompt Optimisation)    | **Available**       | `mlflow>=3.5`. 90x cheaper than fine-tuning.         |


---

## Per-Customer Customisation

Use schema-level isolation — one schema per customer, same notebooks:


| Customer    | Schema            | Doc Types                                     |
| ----------- | ----------------- | --------------------------------------------- |
| FE Fundinfo | `fe_fundinfo`     | KIIDs, Fund Factsheets, Compliance Reports    |
| Finalto     | `finalto`         | Trade Confirmations, FX Forwards, AML Reports |
| Generic     | `governance_demo` | All sample types                              |


---

## Key Gotchas for Regulated Deployments

1. `**ai_parse_document` is batch-optimised** — never call in a for-loop. Process as DataFrame column operations.
2. **DLT streaming tables recompute on full refresh** — use `dlt.create_sink` to avoid re-parsing all documents.
3. **Genie respects UC ABAC** but only with "individual data permissions" mode. Embedded credentials bypass per-user enforcement.
4. **AI/BI Dashboards default to embedded credentials** — switch to individual data permissions for ABAC enforcement.
5. **UC system.access.audit has ~10-15 min latency** (best-effort). Don't build real-time alerting on it.
6. **Time travel bypasses row filters/column masks** — consider disabling for sensitive tables.
7. **Tags don't follow data copies** — if someone creates a table from governed data, ABAC doesn't protect the copy until auto-classification re-scans (up to 24h).
8. **Prompt Registry has a 100K character template limit** and may not appear in UC UI browser yet.

---

## Prerequisites

- Databricks workspace with Unity Catalog (AWS us-east-1 or EU regions)
- Serverless SQL warehouse
- Foundation Model API access (`databricks-claude-sonnet-4`)
- MLflow Prompt Registry preview enabled (for notebooks 05 and production/)
- MLflow 3.5+ (for evaluation + GEPA)
- DBR 16.4+ or Serverless for ABAC policies

## Sample Documents

`sample_docs/` contains three realistic financial documents:

- `kiid_global_equity.pdf` — Key Investor Information Document
- `factsheet_euro_bond.pdf` — Fund Factsheet
- `trade_confirmation.pdf` — FX Forward Trade Confirmation

---

## Demo Flow (20 min)

1. **ABAC** (10 min): Run 00 → show original data → run 03 → show masked + filtered side-by-side
2. **Prompt Registry** (5 min): Show Catalog Explorer Prompts tab → run 05 → promote staging to production
3. **LLM Evaluation** (5 min): Run 04 → show judge scores in MLflow Experiment UI

## Tested On

- FEVM serverless workspace (AWS us-east-1)
- Databricks Runtime 16.4+ / Serverless
- March 2026

