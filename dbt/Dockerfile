FROM python:3.12.16-slim-bookworm
COPY services/processing-python/requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir -r /tmp/requirements.lock && useradd -m -u 10001 dbt
WORKDIR /app/dbt/herbintel
COPY --chown=dbt:dbt dbt/herbintel/ .
USER dbt
ENTRYPOINT ["dbt"]
