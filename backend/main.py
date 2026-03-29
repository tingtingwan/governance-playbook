from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import os

app = FastAPI(title="Regulated Industry Document Processing & Governance")

# --- Models ---
class ParseRequest(BaseModel):
    filename: str

class ExtractRequest(BaseModel):
    parsed_text: str

class PromoteRequest(BaseModel):
    name: str = None

# --- Routes: Documents (Tab 1) ---
@app.get("/api/documents/samples")
def list_samples():
    from .documents import list_sample_docs
    return list_sample_docs()

@app.post("/api/documents/parse")
def parse_document(req: ParseRequest):
    from .documents import parse_document
    return parse_document(req.filename)

# --- Routes: Extract & Compare (Tab 2) ---
@app.post("/api/extract/compare")
def compare_extraction(req: ExtractRequest):
    from .extraction import compare_prompts
    return compare_prompts(req.parsed_text)

# --- Routes: Prompt Management (Tab 3) ---
@app.get("/api/prompts")
def list_prompts():
    from .prompts import list_prompts
    return list_prompts()

@app.get("/api/prompts/versions")
def get_versions():
    from .prompts import get_versions
    return get_versions()

@app.get("/api/prompts/alias/{alias}")
def get_by_alias(alias: str):
    from .prompts import load_by_alias
    return load_by_alias(alias)

@app.post("/api/prompts/promote")
def promote(req: PromoteRequest):
    from .prompts import promote_to_production
    return promote_to_production(req.name)

# --- Routes: Governed Access (Tab 4) ---
@app.get("/api/governance/full")
def governance_full():
    from .data import get_original_data
    return get_original_data()

@app.get("/api/governance/restricted")
def governance_restricted():
    from .data import get_governed_data
    return get_governed_data()

# --- Routes: Evaluation (Tab 5) ---
@app.get("/api/evaluation/runs")
def eval_runs():
    from .evaluation import get_evaluation_runs
    return get_evaluation_runs()

@app.get("/api/evaluation/results")
def eval_results():
    from .evaluation import get_eval_results_from_table
    return get_eval_results_from_table()

# --- Health ---
@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "app_name": os.environ.get("DATABRICKS_APP_NAME", "local"),
        "host": os.environ.get("DATABRICKS_HOST", "not set"),
    }

# --- Static files ---
frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")

@app.get("/")
def serve_index():
    return FileResponse(os.path.join(frontend_dir, "index.html"))

@app.get("/{path:path}")
def serve_static(path: str):
    file_path = os.path.join(frontend_dir, path)
    if os.path.isfile(file_path):
        return FileResponse(file_path)
    return FileResponse(os.path.join(frontend_dir, "index.html"))
