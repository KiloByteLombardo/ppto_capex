"""
API de procesamiento de Prioridades de Pago - Venezuela
Endpoints y conexiones a GCP (BigQuery, GCS)
"""
import os
from pathlib import Path

from flask import Flask, request, jsonify
from dotenv import load_dotenv

# Google Cloud imports
from google.cloud import bigquery
from google.cloud import storage
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2 import service_account

# Procesamiento local
from venezuela import procesar_prioridades_pago

# Cargar variables de entorno
load_dotenv()

# Configuración desde environment
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
BQ_DATASET = os.getenv('BQ_DATASET')
BQ_TABLE = os.getenv('BQ_TABLE')

# Path del archivo de credenciales
CREDENTIALS_PATH = Path(__file__).parent.parent / 'credentials.json'

app = Flask(__name__)


def get_credentials():
    """
    Obtiene credenciales usando ADC (Application Default Credentials).
    Si no está disponible, usa el archivo credentials.json.
    
    Returns:
        Credenciales de Google Cloud
    """
    try:
        # Intentar ADC primero
        credentials, project = default()
        print("[INFO] Usando Application Default Credentials (ADC)")
        return credentials, project or GCP_PROJECT_ID
    except DefaultCredentialsError:
        print("[INFO] ADC no disponible, buscando credentials.json...")
        
        if CREDENTIALS_PATH.exists():
            credentials = service_account.Credentials.from_service_account_file(
                str(CREDENTIALS_PATH)
            )
            print(f"[INFO] Usando credenciales desde: {CREDENTIALS_PATH}")
            return credentials, GCP_PROJECT_ID
        else:
            raise Exception(
                "No se encontraron credenciales. "
                "Configure ADC o proporcione credentials.json"
            )


def get_bigquery_client() -> bigquery.Client:
    """
    Crea y retorna un cliente de BigQuery.
    """
    credentials, project = get_credentials()
    return bigquery.Client(credentials=credentials, project=project)


def get_storage_client() -> storage.Client:
    """
    Crea y retorna un cliente de Google Cloud Storage.
    """
    credentials, project = get_credentials()
    return storage.Client(credentials=credentials, project=project)


# ============================================================================
# ENDPOINTS
# ============================================================================

@app.route('/')
def root():
    """Endpoint raíz con información de la API."""
    return jsonify({
        "name": "PPTO Capex Venezuela API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "health": "/health",
            "test_bigquery": "/test/bigquery",
            "test_gcs": "/test/gcs",
            "test_all": "/test/connections",
            "process": "/process/prioridades-pago"
        }
    })


@app.route('/health')
def health_check():
    """
    Health check endpoint para verificar que la API está funcionando.
    """
    return jsonify({
        "status": "healthy",
        "service": "ppto-capex-venezuela",
        "config": {
            "project_id": GCP_PROJECT_ID or "not_configured",
            "bucket": GCS_BUCKET_NAME or "not_configured",
            "dataset": BQ_DATASET or "not_configured",
            "table": BQ_TABLE or "not_configured"
        }
    })


@app.route('/test/bigquery')
def test_bigquery_connection():
    """
    Prueba la conexión a BigQuery ejecutando una query simple.
    """
    print("[INFO] Probando conexión a BigQuery...")
    
    try:
        client = get_bigquery_client()
        
        # Query simple para probar conexión
        query = "SELECT 1 as test_value"
        query_job = client.query(query)
        results = list(query_job.result())
        
        # Obtener información del dataset si está configurado
        dataset_info = None
        if BQ_DATASET:
            try:
                dataset_ref = client.dataset(BQ_DATASET)
                dataset = client.get_dataset(dataset_ref)
                dataset_info = {
                    "dataset_id": dataset.dataset_id,
                    "location": dataset.location,
                    "created": str(dataset.created)
                }
            except Exception as e:
                dataset_info = {"error": str(e)}
        
        print("[INFO] Conexión a BigQuery exitosa")
        
        return jsonify({
            "status": "connected",
            "service": "BigQuery",
            "project": client.project,
            "test_query_result": results[0].test_value if results else None,
            "dataset_info": dataset_info
        })
        
    except Exception as e:
        print(f"[ERROR] Error conectando a BigQuery: {str(e)}")
        return jsonify({
            "status": "error",
            "service": "BigQuery",
            "error": str(e)
        }), 500


@app.route('/test/gcs')
def test_gcs_connection():
    """
    Prueba la conexión a Google Cloud Storage listando buckets.
    """
    print("[INFO] Probando conexión a GCS...")
    
    try:
        client = get_storage_client()
        
        # Listar buckets para probar conexión
        buckets = list(client.list_buckets(max_results=5))
        bucket_names = [b.name for b in buckets]
        
        # Verificar bucket específico si está configurado
        bucket_info = None
        if GCS_BUCKET_NAME:
            try:
                bucket = client.get_bucket(GCS_BUCKET_NAME)
                bucket_info = {
                    "name": bucket.name,
                    "location": bucket.location,
                    "storage_class": bucket.storage_class
                }
            except Exception as e:
                bucket_info = {"error": str(e)}
        
        print("[INFO] Conexión a GCS exitosa")
        
        return jsonify({
            "status": "connected",
            "service": "Google Cloud Storage",
            "project": client.project,
            "buckets_found": len(bucket_names),
            "sample_buckets": bucket_names[:3],
            "configured_bucket": bucket_info
        })
        
    except Exception as e:
        print(f"[ERROR] Error conectando a GCS: {str(e)}")
        return jsonify({
            "status": "error",
            "service": "Google Cloud Storage",
            "error": str(e)
        }), 500


@app.route('/test/connections')
def test_all_connections():
    """
    Prueba todas las conexiones a servicios de GCP.
    """
    print("[INFO] Probando todas las conexiones...")
    
    results = {
        "bigquery": {"status": "pending"},
        "gcs": {"status": "pending"}
    }
    
    # Test BigQuery
    try:
        client = get_bigquery_client()
        query_job = client.query("SELECT 1")
        list(query_job.result())
        results["bigquery"] = {
            "status": "connected",
            "project": client.project
        }
        print("[INFO] BigQuery: OK")
    except Exception as e:
        results["bigquery"] = {
            "status": "error",
            "error": str(e)
        }
        print(f"[ERROR] BigQuery: {str(e)}")
    
    # Test GCS
    try:
        client = get_storage_client()
        list(client.list_buckets(max_results=1))
        results["gcs"] = {
            "status": "connected",
            "project": client.project
        }
        print("[INFO] GCS: OK")
    except Exception as e:
        results["gcs"] = {
            "status": "error",
            "error": str(e)
        }
        print(f"[ERROR] GCS: {str(e)}")
    
    # Estado general
    all_connected = all(
        r.get("status") == "connected" 
        for r in results.values()
    )
    
    return jsonify({
        "overall_status": "healthy" if all_connected else "degraded",
        "services": results
    })


@app.route('/process/prioridades-pago', methods=['POST'])
def process_prioridades_pago():
    """
    Procesa un archivo Excel de Prioridades de Pago.
    """
    # Verificar que se envió un archivo
    if 'file' not in request.files:
        return jsonify({
            "error": "No se envió ningún archivo",
            "detail": "Debe enviar un archivo con el key 'file'"
        }), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({
            "error": "Nombre de archivo vacío"
        }), 400
    
    print(f"[INFO] Recibido archivo: {file.filename}")
    
    # Validar extensión
    if not file.filename.endswith(('.xlsx', '.xls')):
        return jsonify({
            "error": "El archivo debe ser un Excel (.xlsx o .xls)"
        }), 400
    
    try:
        # Leer contenido del archivo
        content = file.read()
        print(f"[INFO] Tamaño del archivo: {len(content)} bytes")
        
        # Obtener parámetros opcionales
        sheet_name = request.form.get('sheet_name', None)
        
        # Procesar con el módulo de Venezuela (usando threads)
        resultado = procesar_prioridades_pago(content, sheet_name)
        
        if resultado['success']:
            return jsonify(resultado), 200
        else:
            return jsonify(resultado), 422
            
    except Exception as e:
        print(f"[ERROR] Error procesando archivo: {str(e)}")
        return jsonify({
            "error": "Error procesando archivo",
            "detail": str(e)
        }), 500


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Iniciando PPTO Capex Venezuela API")
    print("=" * 60)
    print(f"Project ID: {GCP_PROJECT_ID}")
    print(f"Bucket: {GCS_BUCKET_NAME}")
    print(f"Dataset: {BQ_DATASET}")
    print(f"Table: {BQ_TABLE}")
    print("=" * 60)
    
    app.run(
        host="0.0.0.0",
        port=9777,
        debug=True
    )
