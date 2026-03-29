# Regulated Industry Governance Playbook

Reusable demo for regulated financial services customers showing the full MLOps lifecycle for document processing with governance.

## What It Covers

| Notebook | Capability | Databricks Feature |
|----------|-----------|-------------------|
| `00_setup.sql` | Create sample data, tag PII columns | Unity Catalog Tags |
| `01_abac_masking.sql` | Column masking (email, phone, reference) | UC Column Masks |
| `02_abac_filtering.sql` | Row filtering by jurisdiction | UC Row Filters |
| `03_combined.sql` | Both masking + filtering (compliance vs analyst view) | ABAC |
| `04_prompt_eval.py` | LLM judge evaluation (Correctness + Guidelines) | MLflow Agent Evaluation |
| `05_prompt_registry.py` | Prompt versioning, aliases, promotion | MLflow Prompt Registry |

## Quick Start

1. Upload notebooks to any Databricks workspace
2. Create a serverless SQL warehouse
3. Run notebooks in order: `00` → `01` → `02` → `03`, then `04` and `05` independently
4. All notebooks use widgets — set `catalog` and `schema` to your target location

## Default Configuration

- **Catalog:** `retail_insight_demo_catalog` (change via widget)
- **Schema:** `governance_demo` (change via widget)
- **LLM:** `databricks-claude-sonnet-4` (Foundation Model API)
- **MLflow:** Prompt Registry requires Preview feature enabled

## Per-Customer Customisation

Use schema-level isolation — one schema per customer:

| Customer | Schema |
|----------|--------|
| FE Fundinfo | `fe_fundinfo` |
| Finalto | `finalto` |
| Generic | `governance_demo` |

## Prerequisites

- Databricks workspace with Unity Catalog
- Serverless SQL warehouse
- Foundation Model API access (Claude Sonnet 4)
- MLflow Prompt Registry preview enabled (for notebook 05)
- MLflow 3.1+ (for notebook 04 LLM judges)

## Demo Flow (20 min)

1. **ABAC** (10 min): Run 00 → show original → run 03 → show masked + filtered side-by-side
2. **Prompt Registry** (5 min): Show Catalog Explorer Prompts tab → run 05 → promote staging to production
3. **LLM Evaluation** (5 min): Run 04 → show judge scores in MLflow Experiment UI

## Databricks App

The repo also includes a full-stack **Databricks App** (`app.yaml` + `backend/` + `frontend/`) that wraps all the above into an interactive UI:

| Tab | What it does |
|-----|-------------|
| 1. Parse Document | Upload & parse PDFs via `ai_parse_document` |
| 2. Extract & Compare | Run production vs staging prompts side-by-side |
| 3. Prompt Management | View MLflow Prompt Registry versions, promote aliases |
| 4. Governed Access | Compliance vs analyst view — same table, ABAC-enforced |
| 5. Evaluation | LLM judge scores from MLflow experiments |

### Deploy the App

```bash
databricks apps create governance-demo --app-yaml app.yaml
databricks apps deploy governance-demo
```

### Run Locally

```bash
pip install -r requirements.txt
export DATABRICKS_PROFILE=your-profile
uvicorn backend.main:app --reload --port 8000
```

## Tested On

- FEVM serverless workspace (AWS us-east-1)
- Databricks Runtime 16.4+ / Serverless
- March 2026
