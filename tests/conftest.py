"""Shared pytest fixtures for the IAmDataEng `rest-api-ingestion` rubric.

Boots the Laneway mock API as a uvicorn subprocess on 127.0.0.1:8765 once per
test session, waits for the health endpoint to respond, and tears it down on
exit. Each test that touches state resets the mock's injection counters via
`POST /api/v1/_admin/reset`.

Why a subprocess and not an in-process TestClient?
- The learner code calls a real HTTP server (httpx → 127.0.0.1:8765). We want
  to evaluate the same code path that runs in their dev loop. A TestClient
  would short-circuit httpx's retry/transport layer and let buggy retry code
  pass.
- The subprocess approach is hermetic: no real network, fully deterministic,
  no flakiness from external services.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MOCK_HOST = "127.0.0.1"
MOCK_PORT = 8765
MOCK_BASE = f"http://{MOCK_HOST}:{MOCK_PORT}"
BOOT_TIMEOUT_S = 20.0


def _port_is_free(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((host, port))
            return True
        except OSError:
            return False


def _wait_for_health(url: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            r = httpx.get(url, timeout=1.0)
            if r.status_code == 200:
                return
        except Exception as exc:  # connection refused while booting
            last_err = exc
        time.sleep(0.1)
    raise RuntimeError(
        f"Le mock API n'a pas répondu sur {url} en {timeout}s. "
        f"Dernière erreur: {last_err!r}"
    )


@pytest.fixture(scope="session", autouse=True)
def mock_api_server():
    """Lance le mock API en sous-processus pour toute la session de tests."""
    if not _port_is_free(MOCK_HOST, MOCK_PORT):
        pytest.fail(
            f"Le port {MOCK_PORT} est déjà occupé sur {MOCK_HOST}. "
            "Arrête le service qui tourne dessus (probablement un précédent "
            "uvicorn) et relance pytest."
        )

    env = os.environ.copy()
    # Le sous-processus uvicorn doit pouvoir importer mock_api depuis le repo.
    env["PYTHONPATH"] = str(PROJECT_ROOT) + os.pathsep + env.get("PYTHONPATH", "")

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "mock_api.server:app",
            "--host",
            MOCK_HOST,
            "--port",
            str(MOCK_PORT),
            "--log-level",
            "warning",
        ],
        cwd=PROJECT_ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    try:
        _wait_for_health(f"{MOCK_BASE}/healthz", BOOT_TIMEOUT_S)
    except Exception:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        stderr = b""
        if proc.stderr:
            stderr = proc.stderr.read() or b""
        pytest.fail(
            "Impossible de démarrer le mock API.\n"
            f"stderr uvicorn:\n{stderr.decode(errors='replace')}"
        )

    yield MOCK_BASE

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture()
def reset_mock_state(mock_api_server):
    """Reset les injections du mock avant chaque test qui en dépend."""
    r = httpx.post(f"{mock_api_server}/api/v1/_admin/reset", timeout=2.0)
    r.raise_for_status()
    yield mock_api_server


@pytest.fixture()
def clean_workspace():
    """Supprime bronze/ et silver.duckdb avant un test, pour partir d'un état neuf."""
    bronze = PROJECT_ROOT / "bronze"
    silver = PROJECT_ROOT / "silver.duckdb"
    silver_wal = PROJECT_ROOT / "silver.duckdb.wal"
    if bronze.exists():
        shutil.rmtree(bronze)
    if silver.exists():
        silver.unlink()
    if silver_wal.exists():
        silver_wal.unlink()
    yield PROJECT_ROOT
