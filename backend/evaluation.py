from .config import UC_CATALOG, UC_SCHEMA

try:
    import mlflow
    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False

EXPERIMENT_NAME = f"/{UC_CATALOG}/{UC_SCHEMA}/doc_extraction_eval"


def get_evaluation_runs() -> list[dict]:
    """Fetch recent MLflow evaluation runs."""
    if not MLFLOW_AVAILABLE:
        return [{"error": "MLflow not available"}]
    try:
        mlflow.set_tracking_uri("databricks")
        exp = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
        if not exp:
            # Try user-scoped experiment
            from .config import get_workspace_client
            w = get_workspace_client()
            user = w.current_user.me().user_name
            exp = mlflow.get_experiment_by_name(f"/Users/{user}/doc_extraction_eval")

        if not exp:
            return []

        runs = mlflow.search_runs(
            experiment_ids=[exp.experiment_id],
            order_by=["start_time DESC"],
            max_results=20
        )
        return runs.to_dict(orient="records") if not runs.empty else []
    except Exception as e:
        return [{"error": str(e)}]


def get_eval_results_from_table() -> list[dict]:
    """Fetch evaluation results from the Delta table."""
    try:
        from .data import query_uc
        return query_uc(
            f"SELECT * FROM {UC_CATALOG}.{UC_SCHEMA}.prompt_eval_results ORDER BY doc_id, prompt"
        )
    except Exception as e:
        return [{"error": str(e)}]
