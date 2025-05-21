import os
from datetime import datetime, timezone
import json
import pandas as pd
from google.cloud import secretmanager, storage
import pyodbc

# --- Configuración ---
SECRET_ID = "db-credentials"
BUCKET_NAME = "mi-datalake"
GCS_PREFIX = "temp/stock_actual"

# --- Funciones Auxiliares ---
def _get_secret(secret_id, project_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return json.loads(response.payload.data.decode("UTF-8"))

def _connect_to_db(db_config):
    host = db_config.get('host')
    port = db_config.get('port', '1433')
    user = db_config.get('user')
    password = db_config.get('password')
    database = db_config.get('database')

    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={host},{port};DATABASE={database};UID={user};PWD={password}"
    )
    return pyodbc.connect(connection_string, autocommit=True)

def _read_and_upload_data(conn, project_id):
    sql_query = """
    SELECT
        CodProd, 
        Stock
    FROM ACEROSB1.softland.Dw_VsnpStockalDia
    """ 
    
    df = pd.read_sql(sql_query, conn)
    
    if df.empty:
        return "No data found."

    current_time_utc = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") 
    gcs_full_path = f"{GCS_PREFIX}/dt={current_time_utc}/data.parquet"

    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_full_path)

    from io import BytesIO
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False) 
    parquet_buffer.seek(0)

    blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")
    
    return f"Successfully ingested {len(df)} rows to gs://{BUCKET_NAME}/{gcs_full_path}"

# --- Función Principal (Entrypoint de Cloud Run) ---
def ingest_data(request):
    project_id = os.environ.get("GCP_PROJECT")
    if not project_id:
        return "Error: Project ID not set.", 500

    conn = None
    try:
        db_config = _get_secret(SECRET_ID, project_id)
        conn = _connect_to_db(db_config)
        
        result_message = _read_and_upload_data(conn, project_id)
        
        return result_message, 200

    except Exception as e:
        # En producción, usar logging.error y no devolver detalles sensibles.
        # Aquí, para depuración, devolvemos el error.
        return f"Ingestion failed: {e}", 500
    finally:
        if conn:
            conn.close()