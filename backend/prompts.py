from .config import UC_CATALOG, UC_SCHEMA, PROMPT_NAME

try:
    import mlflow
    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri("databricks-uc")
    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False


def list_prompts() -> list[dict]:
    if not MLFLOW_AVAILABLE:
        return [{"name": PROMPT_NAME, "error": "MLflow not available"}]
    try:
        results = mlflow.genai.search_prompts(
            f"catalog = '{UC_CATALOG}' AND schema = '{UC_SCHEMA}'"
        )
        return [{"name": p.name, "tags": getattr(p, "tags", {})} for p in results]
    except Exception as e:
        return [{"name": PROMPT_NAME, "error": str(e)}]


def get_versions(name: str = None) -> list[dict]:
    name = name or PROMPT_NAME
    versions = []
    for v in range(1, 50):
        try:
            p = mlflow.genai.load_prompt(f"prompts:/{name}/{v}")
            versions.append({
                "version": v,
                "template": p.template,
            })
        except Exception:
            break
    return versions


def load_by_alias(alias: str, name: str = None) -> dict:
    name = name or PROMPT_NAME
    try:
        p = mlflow.genai.load_prompt(f"prompts:/{name}@{alias}")
        return {"version": p.version, "template": p.template, "alias": alias}
    except Exception as e:
        return {"error": str(e), "alias": alias}


def promote_to_production(name: str = None) -> dict:
    name = name or PROMPT_NAME
    staging = load_by_alias("staging", name)
    if "error" in staging:
        return {"error": f"No staging prompt found: {staging['error']}"}

    # Save current production as rollback
    try:
        prod = load_by_alias("production", name)
        if "error" not in prod:
            mlflow.genai.set_prompt_alias(name=name, alias="production_previous", version=prod["version"])
    except Exception:
        pass

    mlflow.genai.set_prompt_alias(name=name, alias="production", version=staging["version"])
    return {"promoted_version": staging["version"], "previous_alias": "production_previous"}
