# Ingest a paginated API, cleanly — `ingestion.rest-api-paginated`

> **Level**: junior · **Estimated time**: ~8 h
> **Framework axes**: `ingestion`, `software_engineering_dataops`

This project drills three skills every junior data engineer must have wired
in before touching an orchestrator or a lakehouse:

1. **Paginate** a REST API with an opaque cursor, and know when to stop.
2. **Survive** `429 Too Many Requests` and `503 Service Unavailable`
   without crashing, without looping, without getting banned.
3. **Be idempotent**: re-running the same ingestion twice must not
   duplicate silver data, nor create duplicate files in bronze.

No tutorial. You read, you code, you push, CI tells you whether it passes —
and on failure, the error message points at the cause.

---

## The context

**Laneway** is a fictional SaaS issue tracker. They expose
`GET /api/v1/issues` with:

- cursor-based pagination (`?cursor=...&limit=100`, 100 max per page),
- a hard rate limit at 60 req/min (returns `429` with a `Retry-After`),
- occasional `503`s from a flaky backend,
- ~8,000 issues, of which ~30% still open (`closed_at: null`) and ~5%
  with a non-empty `labels` field (nested array).

Your job: write an ingestor that paginates fully, retries cleanly, lands
the raw JSON in **bronze** (one file per page, partitioned by ingest
date), then produces a typed, deduplicated DuckDB **silver** table. And
that is idempotent. Without that, the first orchestrator retry in
production doubles your data.

The API doesn't really exist — we ship a **FastAPI mock** that responds
exactly as the spec describes. It runs:

- in CI, as a uvicorn subprocess on `127.0.0.1:8765` (hermetic, zero
  network, zero flakiness),
- locally, either the same way, or in a Docker container via
  `docker-compose up` (port 8080) if you want it to feel more like
  production.

---

## What you ship

| Deliverable | Where |
|---|---|
| The ingestion code | `src/ingest/laneway.py` (public function `ingest_day(date: str) -> None` + CLI) |
| The raw JSON per page | `bronze/laneway/issues/ingested_date=YYYY-MM-DD/page_NNN.json` |
| The typed silver table | `silver.duckdb`, table `issues`, matching `contracts/issues.json` |
| The `.env.example` | With `LANEWAY_BASE_URL` and `LANEWAY_API_KEY` (placeholder only) |

`bronze/` and `silver.duckdb` are gitignored — they're regenerated on every
run by your ingestor. Nobody wants a 2 MB DuckDB binary in git history.

---

## Getting started

If you're in GitHub Codespaces (one-click open from the IAmDataEng app),
the devcontainer has already installed dependencies and copied
`.env.example` to `.env`. Otherwise, locally:

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set up your .env
cp .env.example .env
# edit if needed — the key can be any non-empty string

# 3. (Optional but recommended) Run the mock API locally
#    Option A — background, no Docker
python -m mock_api.server &
#    then export LANEWAY_BASE_URL=http://127.0.0.1:8080 in your .env

#    Option B — via docker-compose, more production-like
docker compose up -d laneway-mock
#    LANEWAY_BASE_URL=http://laneway-mock:8080 (from another container)
#    or http://127.0.0.1:8080 (from the host)

# 4. Run your ingestion (fails until you implement src/)
python -m src.ingest.laneway --date 2026-04-10

# 5. Run the assessment rubric
pytest tests/ -v
```

`pytest` spawns its own mock API subprocess on port `8765`, independent of
the one you may have started for dev — no interference.

Once your 5 tests pass locally, **commit + push** to your fork. GitHub
Actions CI replays the same rubric and the IAmDataEng app displays the
verdict in your dashboard.

---

## The 5 rubric checks

Defined in `tests/test_evaluate.py`. Each failing check prints a clear
pedagogical message pointing at the likely cause.

| # | Id | What we check |
|---|---|---|
| 1 | `full_pagination` | The silver `issues` table contains **exactly 8,000 rows**. Fewer means you stopped too early (typically: wrong stop condition on `next_cursor`). More means your dedup is broken. |
| 2 | `retry_on_429_and_503_observed` | The mock confirms via its admin endpoint that **the 429 on page 3 AND the 503 on page 5 were both triggered**. If you never paginate that far, or if you raise on the first HTTP error, this check fails. |
| 3 | `idempotent_replay` | Re-running `ingest_day(2026-04-10)` does NOT change the silver row count, AND the bronze files are **overwritten byte-for-byte** (md5 identical). No duplicate files, no serialization drift. |
| 4 | `no_plaintext_secrets` | `grep` of the repo only finds `LANEWAY_API_KEY=` as a placeholder in `.env.example`. No real key in plaintext. |
| 5 | `silver_schema_contract` | `PRAGMA table_info('issues')` matches **exactly** the columns AND DuckDB types declared in `contracts/issues.json`. No pandas → DuckDB inference handing you a `DOUBLE` instead of a `DECIMAL`. |

---

## The traps juniors fall into (seen a thousand times in review)

### 1. The pagination stop condition

```python
# Wrong: infinite loop — the API returns `null` (Python None), not the string "null".
while next_cursor != "null":
    ...

# Wrong: exits too early if the API ever returns an empty string.
while next_cursor:
    ...

# Right: the only correct form.
while next_cursor is not None:
    ...
```

### 2. Retry on 429 without backoff

```python
# Wrong: you'll get banned / loop a few thousand times in 2 seconds.
while True:
    r = httpx.get(...)
    if r.status_code == 200:
        break

# Right: bounded exponential backoff, respect Retry-After.
for attempt in range(5):
    r = httpx.get(...)
    if r.status_code == 200:
        break
    if r.status_code in (429, 503):
        retry_after = int(r.headers.get("Retry-After", "1"))
        time.sleep(retry_after * (2 ** attempt))
        continue
    r.raise_for_status()  # any other error => raise
else:
    raise RuntimeError("max retries exceeded")
```

In CI the mock responds instantly (`Retry-After: 1`) — no fear of long
sleeps. In production, those sleeps **must** exist.

### 3. Flatten `labels` by taking `labels[0]`

```python
# Wrong: silent data loss.
"labels": issue["labels"][0] if issue["labels"] else None

# Right: concatenate them all, sorted, stable separator.
"labels": ",".join(sorted(issue["labels"])) if issue["labels"] else None
```

The contract wants `labels` as `VARCHAR`, not `LIST` or `VARCHAR[]`. We
*flatten* for silver — the raw array is still available in bronze if
anyone needs it.

### 4. Non-idempotent bronze

```python
# Wrong: every run creates different files.
fname = f"page_{int(time.time() * 1000)}.json"

# Wrong: not atomic — a network blip leaves you a corrupt JSON.
with open(path, "w") as f:
    json.dump(payload, f)

# Right: deterministic name + atomic write via os.replace.
fname = f"page_{page_number:03d}.json"
tmp = path.with_suffix(".json.tmp")
with tmp.open("w") as f:
    json.dump(payload, f, ensure_ascii=False, sort_keys=True)
os.replace(tmp, path)
```

### 5. Non-idempotent silver

Two strategies the CI accepts:

```sql
-- Strategy A (simplest, works everywhere)
DELETE FROM issues WHERE ingested_date = '2026-04-10';
INSERT INTO issues SELECT ... ;

-- Strategy B (DuckDB ≥ 0.9, also works in Postgres)
INSERT INTO issues (...)
VALUES (...)
ON CONFLICT (issue_id) DO UPDATE SET
    state = EXCLUDED.state,
    closed_at = EXCLUDED.closed_at,
    ...
;
```

Pick one, document the choice in a comment at the top of
`upsert_silver()`. In an interview, you'll be asked why.

### 6. Inferring types from pandas

`con.execute("CREATE TABLE issues AS SELECT * FROM df")` gives you a
table with the types DuckDB *guesses*. Guess what? They aren't what the
contract wants. Declare the `CREATE TABLE issues (...)` by hand with the
exact contract types, then `INSERT INTO issues SELECT ... FROM df` or
`executemany` over typed Python tuples.

---

## Going further (references)

No reading is required to pass this project, but here are the sources that
shaped the rubric:

- Joe Reis & Matt Housley, *Fundamentals of Data Engineering* (O'Reilly,
  2022) — **ch. 7 "Ingestion", pp. 240-255** on API patterns,
  idempotence, and bronze/silver conventions.
- James Densmore, *Data Pipelines Pocket Reference* (O'Reilly, 2021) —
  **ch. 4 "Extracting data"** on pagination, rate-limiting, retry
  patterns.
- Martin Kleppmann, *Designing Data-Intensive Applications* (O'Reilly,
  2017) — **ch. 8 "The Trouble with Distributed Systems"**: why a retry
  must always be *idempotent*, and what that implies on the write path
  (atomic writes, server-side dedup, etc.).
- httpx docs: [Async retry strategies](https://www.python-httpx.org/advanced/retries/)
  and the `httpx.HTTPTransport(retries=N)` tutorial.
- DuckDB docs: [`INSERT ... ON CONFLICT`](https://duckdb.org/docs/sql/statements/insert)
  and [Data Types](https://duckdb.org/docs/sql/data_types/overview).

---

## If you're stuck

The point is for you to struggle a bit — that's what production is. But if
you've been spinning on the same check for more than an hour:

1. Re-read the test error message — it almost always points at the exact
   cause.
2. Run the mock by hand and `curl` it to understand the payloads:
   ```bash
   python -m mock_api.server &
   curl -H "Authorization: Bearer anything" 'http://127.0.0.1:8080/api/v1/issues?limit=3'
   ```
3. Inspect your silver table to see what's off:
   ```bash
   python -c "import duckdb; con = duckdb.connect('silver.duckdb', read_only=True); \
              print(con.execute(\"PRAGMA table_info('issues')\").fetchall()); \
              print(con.execute('SELECT COUNT(*) FROM issues').fetchone())"
   ```
4. Open an issue on your fork with the `help-wanted` label — the
   IAmDataEng community hangs out there.

Good luck.
