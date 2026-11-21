"""Cross-platform command entrypoint. All platform dependencies execute in Docker."""

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(*args):
    subprocess.run(list(args), cwd=ROOT, check=True)


def compose(*args):
    run("docker", "compose", *args)


def worker(*args, integration=False):
    extra = ["-e", "RUN_INTEGRATION=1"] if integration else []
    compose("run", "--rm", "--no-deps", *extra, "worker", *args)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--batch-id")
    args, remainder = parser.parse_known_args()
    command = args.command
    if not (ROOT / ".env").exists():
        (ROOT / ".env").write_bytes((ROOT / ".env.example").read_bytes())
    if command == "build":
        compose("build", "ingestion", "worker", "minio", "storage-init", "airflow-init", "dbt")
    elif command in ("up", "run-airflow"):
        compose("up", "-d", "--build")
    elif command == "down":
        compose("down")
    elif command == "test":
        compose("run", "--rm", "go-check")
        worker("pytest", "-m", "not integration and not cloud", "-q")
        compose(
            "run",
            "--rm",
            "--no-deps",
            "airflow-init",
            "python",
            "-m",
            "unittest",
            "discover",
            "-s",
            "/opt/airflow/tests",
            "-v",
        )
        compose("run", "--rm", "--no-deps", "dbt", "parse")
    elif command == "lint":
        worker("ruff", "check", ".")
        worker("mypy")
        compose("run", "--rm", "go-check")
    elif command == "test-integration":
        worker("pytest", "-m", "integration", "-q", integration=True)
        worker("python", "scripts/acceptance.py", "--count", str(args.count))
        compose("restart", "worker")
        worker("python", "scripts/recovery.py")
    elif command == "acceptance":
        worker("python", "scripts/acceptance.py", "--count", str(args.count), *remainder)
    elif command == "benchmark":
        worker("python", "scripts/benchmark.py", "--count", "90000", *remainder)
    elif command in ("generate-documents", "generate-records", "ingest"):
        worker("python", "-m", "herbintel.cli", command, "--count", str(args.count))
    elif command == "seed":
        worker("python", "-m", "herbintel.cli", "generate-documents", "--count", str(args.count))
        worker("python", "-m", "herbintel.cli", "ingest")
    elif command in ("dbt-parse", "dbt-build", "dbt-test", "dbt-debug", "dbt-deps", "dbt-docs"):
        dbt_cmd = command.removeprefix("dbt-")
        compose(
            "run", "--rm", "--no-deps", "dbt", *(["docs", "generate"] if dbt_cmd == "docs" else [dbt_cmd]), *remainder
        )
    elif command == "snowflake-verify":
        worker("python", "scripts/cloud_verify.py", *(["--batch-id", args.batch_id] if args.batch_id else []))
    elif command in ("validate-data", "export-dataset"):
        if not args.batch_id:
            args.batch_id = json.loads((ROOT / "artifacts/acceptance.json").read_text())["batch_id"]
        worker("python", "scripts/batch_command.py", command, args.batch_id)
    elif command == "failure-tests":
        run(sys.executable, "scripts/failure_tests.py")
    elif command == "verify-dags":
        run(sys.executable, "scripts/verify_dags.py")
    else:
        parser.error("unknown command: " + command)


if __name__ == "__main__":
    main()
