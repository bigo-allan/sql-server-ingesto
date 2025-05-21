FROM python:3.10-slim-buster

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    freetds-dev \
    && rm -rf /var/lib/apt/lists/*

# Copia requirements.txt e instala las librerías de Python.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto del código de la aplicación.
COPY . .

# Establece la variable de entorno del ID del proyecto GCP.
ENV GCP_PROJECT=$GOOGLE_CLOUD_PROJECT

# Comando para iniciar la aplicación.
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:ingest_data"]