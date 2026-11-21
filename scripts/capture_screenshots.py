"""Optional evidence capture: pip install playwright==1.57.0; Microsoft Edge must be installed."""

import base64
import json
from pathlib import Path

from playwright.sync_api import sync_playwright

destination = Path("docs/screenshots")
destination.mkdir(parents=True, exist_ok=True)
with sync_playwright() as playwright:
    browser = playwright.chromium.launch(channel="msedge", headless=True)
    credential = base64.b64encode(b"admin:local-grafana-change-me").decode()
    context = browser.new_context(
        viewport={"width": 1600, "height": 2000},
        device_scale_factor=1,
        extra_http_headers={"Authorization": "Basic " + credential},
    )
    page = context.new_page()
    page.goto("http://localhost:13000/d/herbintel?orgId=1&from=now-1h&to=now&kiosk", wait_until="networkidle")
    page.wait_for_timeout(6000)
    page.screenshot(path=str(destination / "grafana-operations.png"), full_page=True)
    context.close()
    context = browser.new_context(viewport={"width": 1600, "height": 1100}, device_scale_factor=1)
    page = context.new_page()
    page.goto("http://localhost:8081/login/", wait_until="domcontentloaded")
    page.locator("#username").fill("admin")
    page.locator("#password").fill("local-airflow-change-me")
    page.locator('input[type="submit"],button[type="submit"]').click()
    acceptance = json.loads(Path("artifacts/acceptance.json").read_text())
    page.goto(
        "http://localhost:8081/dags/herbintel_document_ingestion/grid?dag_run_id=" + acceptance["airflow_run_id"],
        wait_until="networkidle",
    )
    page.wait_for_timeout(4000)
    page.screenshot(path=str(destination / "airflow-acceptance.png"), full_page=True)
    browser.close()
print("Captured actual Grafana and Airflow screens under docs/screenshots/")
