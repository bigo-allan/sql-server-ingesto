# Usa una imagen base oficial de Python.
# python:3.10 es una buena base, si no funcionó, podemos probar 'debian:buster' o 'debian:bookworm' y construir python encima.
# Pero empecemos con este.
FROM python:3.10

# Establece el directorio de trabajo dentro del contenedor.
WORKDIR /app

# Instala las dependencias del sistema operativo y FreeTDS desde el código fuente para mayor control
# Esto es mucho más robusto que un simple apt-get install freetds-dev
RUN apt-get update -y && apt-get install -y \
    build-essential \
    pkg-config \
    unixodbc-dev \
    libtool \
    autoconf \
    automake \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Descargar, compilar e instalar FreeTDS manualmente
# Versión de FreeTDS. Puedes probar con otras si falla (ej. 1.3.15, 1.2.20)
ENV FREETDS_VERSION 1.3.17 
RUN wget ftp://ftp.freetds.org/pub/freetds/stable/freetds-${FREETDS_VERSION}.tar.gz && \
    tar -xzf freetds-${FREETDS_VERSION}.tar.gz && \
    cd freetds-${FREETDS_VERSION} && \
    ./configure --prefix=/usr/local/freetds --with-tdsver=8.0 --with-unixodbc --disable-shared --enable-static && \
    make && \
    make install && \
    cd /app && \
    rm -rf freetds-${FREETDS_VERSION} freetds-${FREETDS_VERSION}.tar.gz

# Configurar el PATH y LD_LIBRARY_PATH para que pymssql encuentre FreeTDS
ENV PATH="/usr/local/freetds/bin:$PATH"
ENV LD_LIBRARY_PATH="/usr/local/freetds/lib:$LD_LIBRARY_PATH"
ENV CPATH="/usr/local/freetds/include:$CPATH"
ENV LIBRARY_PATH="/usr/local/freetds/lib:$LIBRARY_PATH"
ENV FREETDS=/usr/local/freetds

# Copia requirements.txt e instala las librerías de Python.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto del código de la aplicación.
COPY . .

# Establece la variable de entorno del ID del proyecto GCP.
ENV GCP_PROJECT=$GOOGLE_CLOUD_PROJECT

# Comando para iniciar la aplicación.
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:ingest_data"]