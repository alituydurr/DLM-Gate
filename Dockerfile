# Minimal Alpine Linux runtime for DLM-Gate gate nodes and dashboard
FROM python:3.12-alpine

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Install lightweight web/kafka client dependencies only (core & mutex remain 100% stdlib)
RUN pip install --no-cache-dir fastapi==0.115.5 uvicorn==0.32.1 kafka-python-ng==2.2.3 google-cloud-firestore==2.19.0

COPY src/ /app/src/
COPY tests/ /app/tests/

# Cloud Run injects $PORT (8080); local Docker falls back to 8000
EXPOSE 8000

CMD ["sh", "-c", "uvicorn src.web.app:app --host 0.0.0.0 --port ${PORT:-8000}"]
