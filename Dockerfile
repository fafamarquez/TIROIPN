FROM python:3.12-slim

WORKDIR /app

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar scripts de poblado
COPY scripts/ ./scripts/

# Variables por defecto (se pueden sobreescribir)
ENV DB_HOST=postgres
ENV DB_PORT=5432
ENV DB_NAME=archery_db
ENV DB_USER=archery_user
ENV DB_PASSWORD=archery_pass
ENV NIVEL_POBLADO=leve

CMD ["python", "scripts/poblar_leve.py"]
