FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
RUN pip install griffe==0.47.0

COPY . /app

ENV PYTHONPATH=/app

CMD ["python", "-m", "orchestration.run"]
