FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# System dependencies for psycopg2 and science stack
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    build-essential gcc libpq-dev curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency manifests and install
COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

# Copy source
COPY . /app

ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["uvicorn", "ingestion.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
