FROM python:3.10 

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    freetds-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV GCP_PROJECT=$GOOGLE_CLOUD_PROJECT

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:ingest_data"]