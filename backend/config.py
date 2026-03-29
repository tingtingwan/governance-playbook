import os
from databricks.sdk import WorkspaceClient

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

UC_CATALOG = "retail_insight_demo_catalog"
UC_SCHEMA = "governance_demo"
PROMPT_NAME = f"{UC_CATALOG}.{UC_SCHEMA}.doc_extractor"
LLM_ENDPOINT = "databricks-claude-sonnet-4"


def get_workspace_client() -> WorkspaceClient:
    if IS_DATABRICKS_APP:
        return WorkspaceClient()
    profile = os.environ.get("DATABRICKS_PROFILE", "fevm-retail-insight-demo")
    return WorkspaceClient(profile=profile)


def get_host() -> str:
    if IS_DATABRICKS_APP:
        host = os.environ.get("DATABRICKS_HOST", "")
        return f"https://{host}" if host and not host.startswith("http") else host
    return get_workspace_client().config.host


def get_token() -> str:
    if IS_DATABRICKS_APP:
        token = os.environ.get("DATABRICKS_TOKEN", "")
        if token:
            return token
    w = get_workspace_client()
    auth = w.config.authenticate()
    if auth and "Authorization" in auth:
        return auth["Authorization"].replace("Bearer ", "")
    return ""
