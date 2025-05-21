# Usa una imagen base oficial de Python.
FROM python:3.10

# Establece el directorio de trabajo.
WORKDIR /app

# Instalar el Microsoft ODBC Driver 17 for SQL Server para Debian
# Basado en la documentación oficial de Microsoft:
# https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-odbc-driver-linux?view=sql-server-ver16#debian10
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        curl \
        gnupg \
        unixodbc-dev \
        build-essential && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update -y && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configurar el symbolic link para el driver ODBC si es necesario (a veces msodbcsql17 lo hace automáticamente)
# Puedes verificar si es necesario en los logs si pyodbc no encuentra el driver.
# RUN ln -s /opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.X.so.1.1 /usr/lib/libmsodbcsql-17.so

# Copia requirements.txt e instala las librerías de Python.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto del código de la aplicación.
COPY . .

# Establece la variable de entorno del ID del proyecto GCP.
ENV GCP_PROJECT=$GOOGLE_CLOUD_PROJECT

# Comando para iniciar la aplicación.
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:ingest_data"]