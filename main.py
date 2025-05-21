import os
import json
from datetime import datetime, timezone
import pandas as pd
from google.cloud import secretmanager
from google.cloud import storage
import sqlalchemy
import pymssql 

# --- Configuración de tu función ---
SECRET_ID = "db-credentials" # Nombre del secreto en Secret Manager (Parte 1.3)
PROJECT_ID = os.environ.get("GCP_PROJECT") or os.environ.get("GCP_PROJECT_ID")
BUCKET_NAME = "mi-datalake" # Nombre de tu bucket de GCS (Parte 1.2)
GCS_PREFIX = "temp/stock_actual" # Prefijo de carpeta en GCS (Parte 1.2)

def get_secret(secret_id, project_id):
    """Accede a un secreto almacenado en Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return json.loads(response.payload.data.decode("UTF-8"))

def ingest_data(request):
    """
    Función HTTP de Cloud Run para ingestar datos de SQL Server a GCS Parquet.
    """
    print(f"Iniciando proceso de ingesta para el proyecto: {PROJECT_ID}")

    if not PROJECT_ID:
        print("ERROR: El ID del proyecto no está configurado en el entorno.")
        return "Error interno del servidor: ID del proyecto no configurado.", 500

    db_config = {}
    try:
        db_config = get_secret(SECRET_ID, PROJECT_ID)
        print("Credenciales de base de datos cargadas desde Secret Manager.")
    except Exception as e:
        print(f"ERROR: Fallo al cargar las credenciales de la base de datos desde Secret Manager: {e}")
        return f"Error al cargar credenciales de la base de datos: {e}", 500

    conn = None 
    try:
        host = db_config.get('host')
        port = int(db_config.get('port', 1433))
        user = db_config.get('user')
        password = db_config.get('password')
        database = db_config.get('database')

        if not all([host, user, password, database]):
            raise ValueError("Credenciales de base de datos incompletas (host, user, password, database).")

        print(f"Intentando conectar a SQL Server: {host}:{port}/{database} con usuario {user}...")

        DATABASE_URL = (
            f"mssql+pymssql://{user}:{password}@"
            f"{host}:{port}/{database}"
        )
        engine = sqlalchemy.create_engine(DATABASE_URL)
        conn = engine.connect()
        print("Conexión a SQL Server establecida exitosamente.")

    except ValueError as ve:
        print(f"ERROR DE CONFIGURACIÓN: {ve}")
        return f"Error de configuración de la base de datos: {ve}", 400
    except Exception as e:
        print(f"ERROR DE CONEXIÓN: Fallo al conectar a la base de datos SQL Server: {e}")
        return f"Error al conectar a la base de datos SQL Server: {e}", 500

    # --- Tu Consulta SQL Específica ---
    sql_query = """
    SELECT
        CodProd, 
        Stock
    FROM ACEROSB1.softland.Dw_VsnpStockalDia
    """ 
    print(f"Ejecutando consulta SQL: {sql_query}")

    try:
        df = pd.read_sql(sql_query, conn)
        print(f"Datos leídos: {len(df)} filas.")

        if df.empty:
            print("La consulta no retornó datos. No se generará archivo Parquet.")
            return "Ingesta completada, no se encontraron datos.", 200

        # --- Guardar DataFrame en formato Parquet en GCS ---
        current_time_utc = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") 

        # Construye la ruta final en GCS: mi-datalake/temp/stock_actual/dt=YYYYMMDD_HHMMSS/data.parquet
        gcs_full_path = f"{GCS_PREFIX}/dt={current_time_utc}/data.parquet"

        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(gcs_full_path)

        from io import BytesIO
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False) 
        parquet_buffer.seek(0)

        blob.upload_from_file(parquet_buffer, content_type="application/octet-oct-stream")

        print(f"Archivo Parquet '{gcs_full_path}' subido a GCS exitosamente en el bucket '{BUCKET_NAME}'.")

        return f"Ingesta completada. Archivo en GCS: gs://{BUCKET_NAME}/{gcs_full_path}", 200

    except Exception as e:
        print(f"ERROR CRÍTICO: Fallo durante el proceso de ingesta de datos o subida a GCS: {e}")
        return f"Error durante la ingesta de datos: {e}", 500
    finally:
        if conn:
            try:
                conn.close()
                print("Conexión a base de datos cerrada.")
            except Exception as close_err:
                print(f"ADVERTENCIA: Error al intentar cerrar la conexión a la base de datos: {close_err}")