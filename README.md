# PPTO Capex Venezuela

API para procesamiento de archivos Excel de Prioridades de Pago.

## Características

- 🔍 **Detección automática de cabezales**: Itera por las filas del Excel para encontrar automáticamente los cabezales
- 📊 **Procesamiento con Pandas**: Limpieza y validación de datos usando DataFrames
- ☁️ **Integración GCP**: Conexión a BigQuery y Google Cloud Storage
- 🔐 **Autenticación flexible**: Usa ADC o archivo credentials.json

## Estructura del Proyecto

```
ppto_capex/
├── src/
│   ├── api.py          # Endpoints FastAPI, conexiones a GCP
│   └── venezuela.py    # Lógica de procesamiento del Excel
├── resultados/         # Carpeta para outputs
├── credentials.json    # Credenciales de GCP (opcional)
├── Dockerfile
├── docker-compose.yaml
└── requirements.txt
```

## Configuración

### Variables de Entorno

Crea un archivo `.env` con las siguientes variables:

```env
GCP_PROJECT_ID=tu-proyecto-gcp
GCS_BUCKET_NAME=tu-bucket-gcs
BQ_DATASET=tu_dataset_bigquery
BQ_TABLE=tu_tabla_bigquery
```

### Autenticación con GCP

El proyecto usa el siguiente orden de prioridad para credenciales:

1. **ADC (Application Default Credentials)**: Si tienes `gcloud` configurado
   ```bash
   gcloud auth application-default login
   ```

2. **credentials.json**: Si ADC no está disponible, busca el archivo en la raíz del proyecto

## Instalación

### Local

```bash
# Crear entorno virtual
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac

# Instalar dependencias
pip install -r requirements.txt

# Ejecutar
cd src
python api.py
```

### Docker

```bash
# Construir y ejecutar
docker-compose up --build

# Solo construir
docker build -t ppto-capex-vzla .

# Ejecutar con variables de entorno
docker run -p 8080:8080 --env-file .env ppto-capex-vzla
```

## Endpoints

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | `/` | Información de la API |
| GET | `/health` | Health check |
| GET | `/test/bigquery` | Probar conexión a BigQuery |
| GET | `/test/gcs` | Probar conexión a GCS |
| GET | `/test/connections` | Probar todas las conexiones |
| POST | `/process/prioridades-pago` | Procesar archivo Excel |

## Uso

### Probar conexiones

```bash
# Health check
curl http://localhost:8080/health

# Test BigQuery
curl http://localhost:8080/test/bigquery

# Test GCS
curl http://localhost:8080/test/gcs

# Test todas las conexiones
curl http://localhost:8080/test/connections
```

### Procesar archivo Excel

```bash
curl -X POST http://localhost:8080/process/prioridades-pago \
  -F "file=@Prioridades de Pago.xlsx"
```

## Lógica de Procesamiento (venezuela.py)

El módulo `venezuela.py` contiene la lógica de procesamiento:

1. **`encontrar_cabezales()`**: Itera por las filas buscando la fila de cabezales
2. **`leer_excel_con_cabezales()`**: Lee el Excel con los cabezales detectados
3. **`limpiar_datos()`**: Elimina filas/columnas vacías, normaliza nombres
4. **`validar_estructura()`**: Valida que el archivo tenga la estructura correcta
5. **`procesar_prioridades_pago()`**: Función main que orquesta todo el procesamiento

## Documentación de API

Una vez ejecutando, accede a la documentación interactiva:

- **Swagger UI**: http://localhost:8080/docs
- **ReDoc**: http://localhost:8080/redoc
