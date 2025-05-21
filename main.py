import os
import json
from datetime import datetime, timezone
import pandas as pd
from google.cloud import secretmanager
from google.cloud import storage
import pyodbc # Usaremos pyodbc directamente, sin SQLAlchemy para la conexión
import gunicorn # Importado para asegurar que gunicorn esté disponible si el entorno lo usa.

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
    Función principal HTTP de Cloud Run.
    Se conecta a SQL Server, ejecuta una consulta, convierte a Parquet
    y lo sube a Google Cloud Storage.
    """
    print(f"[{datetime.now(timezone.utc).isoformat()}] Iniciando proceso de ingesta para el proyecto: {PROJECT_ID}")

    if not PROJECT_ID:
        print(f"[{datetime.now(timezone.utc).isoformat()}] ERROR: El ID del proyecto no está configurado en el entorno.")
        return "Error interno del servidor: ID del proyecto no configurado.", 500

    db_config = {}
    try:
        db_config = get_secret(SECRET_ID, PROJECT_ID)
        print(f"[{datetime.now(timezone.utc).isoformat()}] Credenciales de base de datos cargadas desde Secret Manager.")
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] ERROR: Fallo al cargar las credenciales de la base de datos desde Secret Manager: {e}")
        return f"Error al cargar credenciales de la base de datos: {e}", 500

    conn = None # Inicializar la conexión a None
    try:
        host = db_config.get('host')
        port = db_config.get('port', '1433') # Puerto como string para la cadena de conexión ODBC
        user = db_config.get('user')
        password = db_config.get('password')
        database = db_config.get('database')

        if not all([host, user, password, database]):
            raise ValueError("Credenciales de base de datos incompletas (host, user, password, database).")

        print(f"[{datetime.now(timezone.utc).isoformat()}] Intentando conectar a SQL Server: {host}:{port}/{database} con usuario {user}...")
        
        # --- Conexión directa con pyodbc al SQL Server ---
        # Asegúrate de que el nombre del DRIVER sea EXACTO al instalado en el Dockerfile.
        # En este caso, es 'ODBC Driver 17 for SQL Server'
        connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={host},{port};DATABASE={database};UID={user};PWD={password}"
        )
        
        conn = pyodbc.connect(connection_string, autocommit=True) # autocommit es útil para queries de solo lectura
        print(f"[{datetime.now(timezone.utc).isoformat()}] Conexión a SQL Server establecida exitosamente con pyodbc.")

    except pyodbc.Error as db_err:
        sqlstate = db_err.args[0]
        if sqlstate == '08001': # SQLSTATE para error de conexión a la BD
            print(f"[{datetime.now(timezone.utc).isoformat()}] ERROR DE CONEXIÓN CRÍTICO: El servidor o puerto no es accesible, o las credenciales son incorrectas. Error: {db_err}")
        else:
            print(f"[{datetime.now(timezone.utc).isoformat()}] ERROR DE CONEXIÓN SQL Server: {db_err}")
        return f"Error al conectar a la base de datos SQL Server: {db_err}", 500
    except ValueError as ve:
        print(f"[{datetime.now(timezone.utc).isoformat()}] ERROR DE CONFIGURACIÓN: {ve}")
        return f"Error de configuración de la base de datos: {ve}", 400
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] ERROR INESPERADO en la conexión: {e}")
        return f"Error inesperado durante la conexión a la base de datos: {e}", 500

    # --- Tu Consulta SQL Específica ---
    sql_query = """
    SELECT
        CodProd, 
        Stock
    FROM ACEROSB1.softland.Dw_VsnpStockalDia
    """ 
    print(f"[{datetime.now(timezone.utc).isoformat()}] Ejecutando consulta SQL: {sql_query}")

    try:
        # Pandas puede leer directamente de una conexión pyodbc
        df = pd.read_sql(sql_query, conn)
        print(f"[{datetime.now(timezone.utc).isoformat()}] Datos leídos: {len(df)} filas.")

        if df.empty:
            print(f"[{datetime.now(timezone.utc).isoformat()}] La consulta no retornó datos.