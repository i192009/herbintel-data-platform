"""All XCom values are batch identifiers or small verification summaries."""

import os
from datetime import datetime, timedelta
from urllib.parse import urlencode

import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.operators.python import get_current_context

DEFAULTS = {
    "owner": "herbintel",
    "retries": 3,
    "retry_delay": timedelta(seconds=20),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=20),
}


def call(path, payload=None):
    headers = {"Authorization": "Bearer " + os.environ["WORKER_API_KEY"], "X-Org-ID": os.getenv("WORKER_ORG", "demo")}
    url = os.getenv("WORKER_URL", "http://worker:8000") + path
    response = (
        requests.get(url, headers=headers, timeout=30)
        if payload is None
        else requests.post(url, headers=headers, json=payload, timeout=900)
    )
    response.raise_for_status()
    value = response.json()
    if isinstance(value, dict) and value.get("outcome") in ("FAIL", "INCONCLUSIVE"):
        raise AirflowException("quality gate rejected publication")
    return value


def resolve():
    config = get_current_context()["dag_run"].conf or {}
    if config.get("batch_id"):
        return [{"batch_id": config["batch_id"], "pipeline_version": config.get("pipeline_version", "v1")}]
    return [{"batch_id": batch, "pipeline_version": "v1"} for batch in call("/internal/pending")]


@task
def discover_pending_batches():
    return resolve()


@task
def validate_manifest(item):
    call("/internal/validate", item)
    return item


@task
def dispatch_processing(item):
    call("/internal/process", item)
    return item


@task
def check_extraction_results(item):
    call("/internal/quality", item)
    return item


@task
def write_partitioned_parquet(item):
    call("/internal/publish", item)
    return item


@task
def verify_parquet_output(item):
    call("/internal/verify", item)
    return item


@task
def update_batch_status(item):
    call("/internal/finalize", item)
    return item


@dag(
    dag_id="herbintel_document_ingestion",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["herbintel"],
)
def ingestion():
    ids = discover_pending_batches()
    checked = validate_manifest.expand(item=ids)
    processed = dispatch_processing.expand(item=checked)
    quality = check_extraction_results.expand(item=processed)
    written = write_partitioned_parquet.expand(item=quality)
    verified = verify_parquet_output.expand(item=written)
    update_batch_status.expand(item=verified)


@task
def trigger_warehouse_if_configured(item):
    result = call("/warehouse/configured", item)
    if not result["configured"]:
        raise AirflowSkipException("Snowflake credentials unavailable")
    from airflow.api.common.trigger_dag import trigger_dag

    trigger_dag(dag_id="herbintel_warehouse_loading", conf=item, replace_microseconds=False)


@task
def publish_curated_manifest(item):
    call("/datasets/export", item)
    return item


@dag(
    dag_id="herbintel_incremental_etl",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
)
def incremental():
    ids = discover_pending_batches.override(task_id="identify_new_or_changed")()
    checked = validate_manifest.expand(item=ids)
    done = dispatch_processing.override(task_id="process_required_versions").expand(item=checked)
    written = write_partitioned_parquet.expand(item=check_extraction_results.expand(item=done))
    curated = publish_curated_manifest.expand(item=verify_parquet_output.expand(item=written))
    final = update_batch_status.expand(item=curated)
    trigger_warehouse_if_configured.expand(item=final)


@task
def warehouse_step(item, step):
    call("/warehouse/" + step, item)
    return item


@dag(
    dag_id="herbintel_warehouse_loading",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
)
def warehouse():
    ids = discover_pending_batches()
    connection = (
        warehouse_step.override(task_id="validate_snowflake_connection").partial(step="connect").expand(item=ids)
    )
    staged = warehouse_step.override(task_id="verify_staged_files").partial(step="stage").expand(item=connection)
    loaded = warehouse_step.override(task_id="load_records").partial(step="load").expand(item=staged)
    history = warehouse_step.override(task_id="inspect_load_history").partial(step="history").expand(item=loaded)
    verified = warehouse_step.override(task_id="verify_loaded_counts").partial(step="verify").expand(item=history)
    warehouse_step.override(task_id="run_dbt").partial(step="dbt").expand(item=verified)


@dag(
    dag_id="herbintel_data_quality",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULTS,
)
def data_quality():
    @task
    def select_published_batches():
        config = get_current_context()["dag_run"].conf or {}
        if config.get("batch_id"):
            return [{"batch_id": config["batch_id"], "pipeline_version": config.get("pipeline_version", "v1")}]
        return [{"batch_id": batch, "pipeline_version": "v1"} for batch in call("/internal/recent")]

    check_extraction_results.override(task_id="counts_duplicates_schema_nulls_freshness").expand(
        item=select_published_batches()
    )


@task
def export_training(item):
    config = get_current_context()["dag_run"].conf or {}
    value = call(
        "/datasets/export",
        {**item, "seed": config.get("seed", 42), "dataset_id": config.get("dataset_id", "synthetic-reference")},
    )
    return {"manifest_key": value["manifest_key"], "rows": value["rows"], "dataset_version": value["dataset_version"]}


@dag(
    dag_id="herbintel_training_dataset_export",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULTS,
)
def training_export():
    export_training.expand(item=discover_pending_batches())


@task
def select_historical():
    config = get_current_context()["dag_run"].conf or {}
    if not config.get("pipeline_version"):
        raise AirflowException("backfill requires an explicit pipeline_version")
    batches = config.get("batch_ids")
    if not batches and config.get("start_date") and config.get("end_date"):
        batches = call("/internal/history?" + urlencode({"start": config["start_date"], "end": config["end_date"]}))
    if not batches:
        raise AirflowException("provide batch_ids or a nonempty start_date/end_date range")
    return [{"batch_id": value, "pipeline_version": config["pipeline_version"]} for value in batches]


@dag(
    dag_id="herbintel_historical_backfill",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
)
def historical_backfill():
    selected = select_historical()
    processed = dispatch_processing.expand(item=validate_manifest.expand(item=selected))
    written = write_partitioned_parquet.expand(item=check_extraction_results.expand(item=processed))
    update_batch_status.override(task_id="reconcile_preserve_versions").expand(
        item=verify_parquet_output.expand(item=written)
    )


ingestion()
incremental()
warehouse()
data_quality()
training_export()
historical_backfill()
