import os
from databricks import sql as dbsql
from .config import get_workspace_client, get_host, get_token, UC_CATALOG, UC_SCHEMA


def _get_sql_connection():
    host = get_host().replace("https://", "").replace("http://", "")
    token = get_token()
    w = get_workspace_client()

    # Find a running SQL warehouse
    warehouses = w.warehouses.list()
    wh_id = None
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            wh_id = wh.id
            break
    if not wh_id:
        for wh in warehouses:
            wh_id = wh.id
            break

    if not wh_id:
        raise RuntimeError("No SQL warehouse found")

    return dbsql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{wh_id}",
        access_token=token,
    )


def query_uc(sql: str) -> list[dict]:
    conn = _get_sql_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


def get_original_data() -> list[dict]:
    return query_uc(
        f"SELECT * FROM {UC_CATALOG}.{UC_SCHEMA}.client_documents ORDER BY doc_id"
    )


def get_governed_data() -> list[dict]:
    return query_uc(
        f"SELECT * FROM {UC_CATALOG}.{UC_SCHEMA}.client_documents_governed ORDER BY doc_id"
    )
