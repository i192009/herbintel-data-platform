FROM python:3.12.16-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/services/processing-python
RUN apt-get update && apt-get install -y --no-install-recommends tesseract-ocr tesseract-ocr-chi-sim poppler-utils fonts-dejavu-core && rm -rf /var/lib/apt/lists/* && useradd --uid 10001 --create-home worker
WORKDIR /app
COPY services/processing-python/requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir -r /tmp/requirements.lock
COPY --chown=worker:worker . /app
RUN mkdir -p /app/artifacts /app/datasets/generated && chown worker:worker /app && chown -R worker:worker /app/artifacts /app/datasets
USER worker
CMD ["uvicorn","herbintel.api:app","--host","0.0.0.0","--port","8000"]
