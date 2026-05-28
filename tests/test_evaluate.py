"""IAmDataEng — rubric d'évaluation pour `ingestion.rest-api-paginated`.

Cinq checks déterministes alignés sur la spec produit (`docs/PROJECTS_CATALOG_V1.md`
§ ingestion.rest-api-paginated) :

  1. full_pagination          — toutes les pages consommées
  2. retry_on_429_observed    — retry sur 429/503 effectivement déclenché
  3. idempotent_replay        — relancer ne duplique ni en bronze ni en silver
  4. no_plaintext_secrets     — aucun token réel committé en clair
  5. silver_schema_contract   — table silver conforme à contracts/issues.json

Tous les tests passent par le mock API local (sous-processus uvicorn lancé par
conftest.py) — aucune dépendance réseau, aucune flakiness possible.

Chaque échec produit un message FR pédagogique pointant la cause probable.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import httpx
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONTRACT_PATH = PROJECT_ROOT / "contracts" / "issues.json"
SILVER_DB = PROJECT_ROOT / "silver.duckdb"
BRONZE_ROOT = PROJECT_ROOT / "bronze" / "laneway" / "issues"
INGEST_DATE = "2026-04-10"
EXPECTED_TOTAL_ROWS = 8_000  # cf. mock_api/data.py TOTAL_ISSUES

# Le mock écoute sur 127.0.0.1:8765 (cf. conftest.py).
MOCK_BASE = "http://127.0.0.1:8765"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_ingest(date: str = INGEST_DATE) -> subprocess.CompletedProcess:
    """Exécute `python -m src.ingest.laneway --date <date>` dans un sous-processus.

    On passe par subprocess plutôt qu'un import direct pour reproduire à
    l'identique ce que fait un learner en ligne de commande, et pour ne pas
    polluer l'état du process pytest (connexion DuckDB ouverte, etc.).
    """
    env = os.environ.copy()
    env["LANEWAY_BASE_URL"] = MOCK_BASE
    env["LANEWAY_API_KEY"] = "ci-test-token-not-a-real-secret"
    env["INGEST_DATE"] = date
    env["PYTHONPATH"] = str(PROJECT_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.run(
        [sys.executable, "-m", "src.ingest.laneway", "--date", date],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
    )


def _fail_with_logs(message: str, proc: subprocess.CompletedProcess) -> None:
    pytest.fail(
        f"{message}\n"
        f"--- stdout ---\n{proc.stdout}\n"
        f"--- stderr ---\n{proc.stderr}\n"
        "Astuce : implémente `iter_pages`, `write_bronze_page`, `upsert_silver`, "
        "`ingest_day` dans src/ingest/laneway.py — ils lèvent NotImplementedError "
        "par défaut."
    )


def _load_contract() -> dict:
    with CONTRACT_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def _normalize_duckdb_type(t: str) -> str:
    return t.replace(" ", "").upper()


def _file_md5(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def _list_bronze_pages(date: str) -> list[Path]:
    partition = BRONZE_ROOT / f"ingested_date={date}"
    if not partition.exists():
        return []
    return sorted(partition.glob("page_*.json"))


def _wipe_workspace() -> None:
    if (PROJECT_ROOT / "bronze").exists():
        shutil.rmtree(PROJECT_ROOT / "bronze")
    if SILVER_DB.exists():
        SILVER_DB.unlink()
    wal = PROJECT_ROOT / "silver.duckdb.wal"
    if wal.exists():
        wal.unlink()


def _snapshot_injections() -> dict:
    r = httpx.get(f"{MOCK_BASE}/api/v1/_admin/injections", timeout=2.0)
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Fixture : on lance l'ingestor UNE fois pour les checks 1, 2 et 5.
# Les checks 3 (idempotent_replay) et 4 (no_plaintext_secrets) sont autonomes.
# On capture l'état des injections JUSTE après le run pour que le check 2
# reste valide même si un test ultérieur reset le mock.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def after_first_ingest():
    # Reset mock state at the start of this fixture so the 429/503 injections
    # fire fresh on the first run (the learner must observe and retry them).
    httpx.post(f"{MOCK_BASE}/api/v1/_admin/reset", timeout=2.0).raise_for_status()

    _wipe_workspace()

    proc = _run_ingest()
    if proc.returncode != 0:
        _fail_with_logs(
            "Le pipeline `python -m src.ingest.laneway` a planté (exit code != 0).",
            proc,
        )

    if not SILVER_DB.exists():
        _fail_with_logs(
            "Le pipeline s'est terminé sans erreur mais n'a pas créé silver.duckdb. "
            "Vérifie que `upsert_silver()` ouvre bien `duckdb.connect('silver.duckdb')` "
            "et écrit la table `issues`.",
            proc,
        )

    # Capture le snapshot des injections AVANT que d'autres tests ne resettent
    # le mock. Le test `retry_on_429_and_503_observed` lit ce snapshot et non
    # l'état live — donc l'ordre des tests n'a pas d'influence.
    injections_snapshot = _snapshot_injections()

    yield {
        "process": proc,
        "injections": injections_snapshot,
    }


# ---------------------------------------------------------------------------
# Check 1 — full_pagination
# ---------------------------------------------------------------------------


def test_full_pagination(after_first_ingest):
    """Silver row count == 8000 (total connu du mock)."""
    con = duckdb.connect(str(SILVER_DB), read_only=True)
    try:
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        if "issues" not in tables:
            pytest.fail(
                "La table `issues` n'existe pas dans silver.duckdb. "
                "Astuce : ton CREATE TABLE doit s'appeler exactement `issues`."
            )
        count = con.execute("SELECT COUNT(*) FROM issues").fetchone()[0]
    finally:
        con.close()

    if count != EXPECTED_TOTAL_ROWS:
        if count < EXPECTED_TOTAL_ROWS:
            diff = EXPECTED_TOTAL_ROWS - count
            hint = (
                f"Il te manque {diff} lignes. Le piège classique : la condition "
                "d'arrêt de ta pagination. L'API renvoie `next_cursor: null` "
                "(JSON null, désérialisé en `None` côté Python), PAS la chaîne "
                "`\"null\"`. Si tu fais `while next_cursor != \"null\"`, tu boucles "
                "à l'infini. Si tu fais `while next_cursor`, tu sors trop tôt sur "
                "une chaîne vide. La bonne condition : `while next_cursor is not None`."
            )
        else:
            diff = count - EXPECTED_TOTAL_ROWS
            hint = (
                f"Tu as {diff} lignes en trop. Tu insères probablement plusieurs "
                "fois le même issue_id (re-lecture des mêmes pages, ou pas de "
                "dédoublonnage avant insert). Ajoute un dédoublonnage sur "
                "`issue_id` côté Python ou un `ON CONFLICT (issue_id) DO NOTHING` "
                "côté DuckDB."
            )
        pytest.fail(
            f"Row count silver incorrect — attendu {EXPECTED_TOTAL_ROWS}, "
            f"obtenu {count}.\n{hint}"
        )


# ---------------------------------------------------------------------------
# Check 2 — retry_on_429_observed
# ---------------------------------------------------------------------------


def test_retry_on_429_and_503_observed(after_first_ingest):
    """Le mock confirme que 429 et 503 ont chacun été injectés ET que le run a quand même fini."""
    data = after_first_ingest["injections"]
    fires = data["injection_fires"]

    # Les injections du mock (cf. mock_api/server.py INJECTIONS) :
    #   offset 200 -> 429
    #   offset 400 -> 503
    fired_429 = fires.get("200", 0)
    fired_503 = fires.get("400", 0)

    if fired_429 == 0:
        pytest.fail(
            "Aucun 429 n'a été injecté pendant l'ingestion — soit ton client n'a "
            "jamais atteint la page 3 (offset 200), soit il a court-circuité la "
            "pagination. Vérifie que tu paginates bien tant que "
            "`next_cursor is not None` et que tu n'arrêtes pas sur la première "
            "erreur HTTP."
        )
    if fired_503 == 0:
        pytest.fail(
            "Aucun 503 n'a été injecté pendant l'ingestion — soit ton client n'a "
            "jamais atteint la page 5 (offset 400), soit il a court-circuité la "
            "pagination. Même cause probable que pour le 429 ci-dessus."
        )

    # Si on arrive ici, le mock a renvoyé 429 puis 503. Comme le test
    # `test_full_pagination` exige 8000 rows en silver, on sait par
    # conjonction que le client a retry ET récupéré (le mock ne renvoie
    # l'erreur qu'une seule fois par offset).

    # Sanity check : pas de tempête de requêtes.
    # 80 pages * 1 hit + 2 retries = ~82 hits sur /issues. Marge large.
    total = data["total_requests"]
    if total > 200:
        pytest.fail(
            f"Le mock a reçu {total} requêtes pour ingérer 8000 issues en pages "
            "de 100. C'est anormalement élevé — tu retry probablement trop "
            "agressivement (boucle infinie sur 429 ?) ou tu re-paginates depuis "
            "zéro à chaque erreur. Utilise un backoff exponentiel borné (5 "
            "tentatives max par requête) et ne rejoue QUE la requête échouée."
        )


# ---------------------------------------------------------------------------
# Check 3 — idempotent_replay
# ---------------------------------------------------------------------------


def test_idempotent_replay():
    """Relancer ingest_day(2026-04-10) ne change pas le row count silver
    ET les fichiers bronze sont overwrités byte-for-byte (pas dupliqués)."""
    _wipe_workspace()

    # Reset des injections du mock pour repartir d'un état déterministe.
    httpx.post(f"{MOCK_BASE}/api/v1/_admin/reset", timeout=2.0).raise_for_status()

    proc1 = _run_ingest()
    if proc1.returncode != 0:
        _fail_with_logs("Premier run en échec.", proc1)

    pages_1 = _list_bronze_pages(INGEST_DATE)
    if not pages_1:
        pytest.fail(
            "Aucun fichier `page_*.json` trouvé dans "
            f"bronze/laneway/issues/ingested_date={INGEST_DATE}/. "
            "Vérifie que `write_bronze_page()` crée bien la structure Hive "
            "`ingested_date=YYYY-MM-DD/page_NNN.json` à la racine `bronze/laneway/issues/`."
        )
    hashes_1 = {p.name: _file_md5(p) for p in pages_1}

    con = duckdb.connect(str(SILVER_DB), read_only=True)
    try:
        rows_1 = con.execute("SELECT COUNT(*) FROM issues").fetchone()[0]
    finally:
        con.close()

    # Reset des injections pour que le second run revoie aussi 429+503. Cela
    # vérifie en passant que l'idempotence tient même quand les retries se
    # rejouent (au cas où le learner aurait calé son idempotence sur "pas
    # d'erreur").
    httpx.post(f"{MOCK_BASE}/api/v1/_admin/reset", timeout=2.0).raise_for_status()

    proc2 = _run_ingest()
    if proc2.returncode != 0:
        _fail_with_logs("Second run en échec.", proc2)

    pages_2 = _list_bronze_pages(INGEST_DATE)
    hashes_2 = {p.name: _file_md5(p) for p in pages_2}

    con = duckdb.connect(str(SILVER_DB), read_only=True)
    try:
        rows_2 = con.execute("SELECT COUNT(*) FROM issues").fetchone()[0]
    finally:
        con.close()

    # Row count silver inchangé
    if rows_1 != rows_2:
        pytest.fail(
            f"Le row count silver a changé entre les deux runs : "
            f"{rows_1} -> {rows_2} (delta = {rows_2 - rows_1}).\n"
            "Stratégies acceptées pour l'idempotence :\n"
            f"  1) DELETE FROM issues WHERE ingested_date = '{INGEST_DATE}' avant l'INSERT, OU\n"
            "  2) INSERT ... ON CONFLICT (issue_id) DO UPDATE SET ... (DuckDB >= 0.9).\n"
            "Un retry d'orchestrateur ne doit JAMAIS dupliquer la donnée."
        )

    # Bronze : pas de fichiers en plus, pas de fichiers manquants
    extra = sorted(set(hashes_2) - set(hashes_1))
    missing = sorted(set(hashes_1) - set(hashes_2))
    if extra or missing:
        pytest.fail(
            "Les fichiers bronze diffèrent entre les deux runs.\n"
            f"En trop au run 2 : {extra}\n"
            f"Manquants au run 2 : {missing}\n"
            "Tu utilises probablement un nom de fichier basé sur l'horodatage "
            "(`page_<timestamp>.json`) au lieu d'un nom déterministe basé sur le "
            "numéro de page (`page_001.json`, `page_002.json`, ...). Le second "
            "run doit OVERWRITE les mêmes fichiers, pas en créer de nouveaux."
        )

    # Bronze : même contenu byte-for-byte
    diff = [name for name in hashes_1 if hashes_1[name] != hashes_2.get(name)]
    if diff:
        sample = diff[:5]
        suffix = "..." if len(diff) > 5 else ""
        pytest.fail(
            f"{len(diff)} fichier(s) bronze ont un hash MD5 différent au second "
            f"run : {sample}{suffix}.\n"
            "L'écriture bronze n'est pas déterministe. Causes typiques :\n"
            "  - Tu mets un timestamp `ingested_at` dans le JSON (à enlever).\n"
            "  - Tu sérialises sans `sort_keys=True` (ordre des clés variable).\n"
            "  - Tu mélanges les modes d'écriture (`indent=2` vs compact).\n"
            "Le JSON brut doit refléter la réponse de l'API, sérialisé avec une "
            "convention stable (`json.dumps(payload, ensure_ascii=False, "
            "sort_keys=True)`)."
        )


# ---------------------------------------------------------------------------
# Check 4 — no_plaintext_secrets
# ---------------------------------------------------------------------------


# Placeholders acceptés dans .env.example (regex sur la VALEUR après le `=`).
_PLACEHOLDER_RX = re.compile(
    r"^(replace[-_]?me|your[-_]|xxx+|change[-_]?me|placeholder|todo|<.*>)",
    re.IGNORECASE,
)


def _scan_for_secret(root: Path) -> list[tuple[Path, int, str]]:
    """Retourne les occurrences suspectes de LANEWAY_API_KEY=<valeur> dans le repo."""
    skip_dirs = {".git", ".venv", "venv", "node_modules", "__pycache__",
                 ".pytest_cache", ".mypy_cache", ".ruff_cache", "bronze"}
    pattern = re.compile(r"LANEWAY_API_KEY\s*=\s*([^\s\n\r#]+)")
    findings: list[tuple[Path, int, str]] = []

    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if any(part in skip_dirs for part in path.parts):
            continue
        if path.suffix in {".pyc", ".so", ".duckdb", ".wal", ".png", ".jpg",
                           ".jpeg", ".pdf", ".zip", ".gz", ".tar"}:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            for m in pattern.finditer(line):
                value = m.group(1).strip().strip("\"'")
                # .env.example accepte un placeholder évident.
                if path.name == ".env.example" and _PLACEHOLDER_RX.match(value):
                    continue
                findings.append((path.relative_to(root), lineno, value))
    return findings


def test_no_plaintext_secrets():
    """Aucune clé API en clair committed dans le repo (hors placeholder de .env.example)."""
    findings = _scan_for_secret(PROJECT_ROOT)
    if findings:
        lines = "\n".join(
            f"  - {p}:{lineno}  ->  LANEWAY_API_KEY={value}"
            for p, lineno, value in findings
        )
        pytest.fail(
            "Une (ou plusieurs) clé API en clair a été détectée dans le repo :\n"
            f"{lines}\n\n"
            "Règle : aucun secret committé. Mets `LANEWAY_API_KEY=<placeholder>` "
            "dans `.env.example` uniquement (ex. `replace-me-with-any-non-empty-string`), "
            "et utilise un vrai `.env` local non commité (déjà dans `.gitignore`). "
            "Le code doit lire la clé depuis `os.environ['LANEWAY_API_KEY']`."
        )


# ---------------------------------------------------------------------------
# Check 5 — silver_schema_contract
# ---------------------------------------------------------------------------


def test_silver_schema_contract(after_first_ingest):
    """La table silver `issues` matche EXACTEMENT contracts/issues.json."""
    contract = _load_contract()
    expected_cols = [
        (c["name"], _normalize_duckdb_type(c["duckdb_type"])) for c in contract["columns"]
    ]

    con = duckdb.connect(str(SILVER_DB), read_only=True)
    try:
        rows = con.execute("PRAGMA table_info('issues')").fetchall()
    finally:
        con.close()

    # PRAGMA table_info returns (cid, name, type, notnull, dflt_value, pk)
    actual = [(r[1], _normalize_duckdb_type(r[2])) for r in rows]

    if actual != expected_cols:
        expected_names = [n for n, _ in expected_cols]
        actual_names = [n for n, _ in actual]
        msg = [
            "Le schéma de la table silver `issues` ne correspond pas au contrat.",
            f"Attendu : {expected_cols}",
            f"Obtenu  : {actual}",
        ]
        if set(actual_names) != set(expected_names):
            missing = [n for n in expected_names if n not in actual_names]
            extra = [n for n in actual_names if n not in expected_names]
            if missing:
                msg.append(f"Colonnes manquantes : {missing}")
            if extra:
                msg.append(f"Colonnes en trop    : {extra}")
        elif actual_names != expected_names:
            msg.append(
                "Ordre des colonnes incorrect.\n"
                f"  Attendu : {expected_names}\n"
                f"  Obtenu  : {actual_names}\n"
                "Déclare les colonnes dans `CREATE TABLE` dans l'ordre du contrat."
            )
        else:
            mismatched = [
                (e[0], e[1], a[1]) for e, a in zip(expected_cols, actual) if e != a
            ]
            msg.append(f"Types incorrects (col, attendu, obtenu) : {mismatched}")
        msg.append(
            "Ne laisse pas DuckDB inférer les types depuis un DataFrame — déclare "
            "explicitement le schéma dans `CREATE TABLE issues (...)` avec les "
            "types exacts du contrat (`BIGINT`, `TIMESTAMP`, `VARCHAR`, `DATE`...). "
            "Indice spécifique : `labels` est `VARCHAR` (CSV-joined trié), PAS un "
            "`VARCHAR[]` ni un `LIST` — on aplatit pour le silver."
        )
        pytest.fail("\n".join(msg))
