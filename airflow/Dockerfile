FROM apache/airflow:2.11.2-python3.12
RUN pip install --no-cache-dir statsd==4.0.1
COPY --chown=airflow:root airflow/dags/ /opt/airflow/dags/
COPY --chown=airflow:root airflow/tests/ /opt/airflow/tests/
