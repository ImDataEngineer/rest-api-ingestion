# Ingérer une API paginée, proprement — `ingestion.rest-api-paginated`

> **Niveau** : junior · **Durée estimée** : ~8 h
> **Axes framework** : `ingestion`, `software_engineering_dataops`

Ce projet exerce trois compétences que tout data engineer junior doit avoir
ancrées avant de toucher à un orchestrateur ou à un lakehouse :

1. **Paginer** une API REST avec un curseur opaque, et savoir s'arrêter.
2. **Survivre** aux `429 Too Many Requests` et aux `503 Service Unavailable`
   sans tomber, sans boucler, sans se faire bannir.
3. **Être idempotent** : relancer la même ingestion deux fois ne doit ni
   dupliquer la donnée en silver, ni créer de fichiers en double en bronze.

Pas de tutoriel. Tu lis, tu codes, tu pousses, la CI te dit si ça passe — et
si ça casse, le message d'erreur te pointe la cause.

---

## Le contexte

**Laneway** est un issue-tracker SaaS fictif. Ils exposent
`GET /api/v1/issues` avec :

- pagination par curseur (`?cursor=...&limit=100`, 100 max par page),
- rate limit dur à 60 req/min (renvoie `429` avec un `Retry-After`),
- des `503` occasionnels sur leur backend qui hoquette,
- ~8 000 issues, dont ~30 % toujours ouvertes (`closed_at: null`) et ~5 %
  avec un champ `labels` non-vide (tableau imbriqué).

Ton job : écrire un ingestor qui pagine entièrement, retry proprement,
landed le JSON brut en **bronze** (un fichier par page, partitionné par
date d'ingestion), puis produit une table **silver** DuckDB typée et
déduplifiée. Et qui est idempotente. Sans ça, en prod, le premier retry
de ton orchestrateur double la donnée.

L'API n'existe pas vraiment — on te fournit un **mock FastAPI** qui répond
exactement comme la spec décrit. Il tourne :

- en CI, en sous-processus uvicorn sur `127.0.0.1:8765` (hermétique, zéro
  réseau, zéro flakiness),
- en local, soit pareil, soit dans un conteneur Docker via
  `docker-compose up` (port 8080) si tu veux le faire ressembler à la prod.

---

## Ce que tu vas livrer

| Livrable | Où |
|---|---|
| Le code d'ingestion | `src/ingest/laneway.py` (fonction publique `ingest_day(date: str) -> None` + CLI) |
| Le JSON brut par page | `bronze/laneway/issues/ingested_date=YYYY-MM-DD/page_NNN.json` |
| La table silver typée | `silver.duckdb`, table `issues` conforme à `contracts/issues.json` |
| Le `.env.example` | Avec `LANEWAY_BASE_URL` et `LANEWAY_API_KEY` (placeholder seulement) |

`bronze/` et `silver.duckdb` sont gitignorés — ils sont régénérés à chaque
run par ton ingestor. Personne ne veut un binaire DuckDB de 2 Mo dans
l'historique git.

---

## Comment commencer

Si tu es dans GitHub Codespaces (ouverture en un clic depuis l'app
IAmDataEng), le devcontainer a déjà installé les dépendances et copié
`.env.example` en `.env`. Sinon, en local :

```bash
# 1. Installer les dépendances
pip install -r requirements.txt

# 2. Préparer ton .env
cp .env.example .env
# édite si besoin — la clé peut être n'importe quelle chaîne non vide

# 3. (Optionnel mais recommandé) Lancer le mock API en local
#    Option A — en arrière-plan, sans Docker
python -m mock_api.server &
#    puis exporte LANEWAY_BASE_URL=http://127.0.0.1:8080 dans ton .env

#    Option B — via docker-compose, ça ressemble plus à la prod
docker compose up -d laneway-mock
#    LANEWAY_BASE_URL=http://laneway-mock:8080 (depuis un autre conteneur)
#    ou http://127.0.0.1:8080 (depuis l'hôte)

# 4. Lancer ton ingestion (cassera tant que tu n'as pas implémenté src/)
python -m src.ingest.laneway --date 2026-04-10

# 5. Lancer la rubric d'évaluation
pytest tests/ -v
```

`pytest` lance son propre mock API en sous-processus sur le port `8765`,
indépendamment de celui que tu aurais démarré pour ton dev — pas
d'interférence.

Quand tes 5 tests passent en local, **commit + push** sur ton fork. La
CI GitHub Actions rejoue la même rubric et l'app IAmDataEng affiche le
verdict dans ton dashboard.

---

## Les 5 checks de la rubric

Définis dans `tests/test_evaluate.py`. Chaque check qui échoue te crache
un message FR pédagogique pointant la cause probable.

| # | Id | Ce qu'on vérifie |
|---|---|---|
| 1 | `full_pagination` | La table silver `issues` contient **exactement 8 000 lignes**. Si tu en as moins, tu t'arrêtes trop tôt (typiquement : mauvaise condition d'arrêt sur `next_cursor`). Si tu en as plus, ton dédoublonnage est cassé. |
| 2 | `retry_on_429_and_503_observed` | Le mock confirme via son endpoint admin que **le 429 sur la page 3 ET le 503 sur la page 5 ont bien été déclenchés**. Si tu ne paginates jamais jusqu'à ces offsets, ou si tu lèves sur la première erreur HTTP, ce check te tombe dessus. |
| 3 | `idempotent_replay` | Relancer `ingest_day(2026-04-10)` une seconde fois ne change PAS le row count silver, ET les fichiers bronze sont **overwrités byte-for-byte** (md5 identique). Pas de fichier en double, pas de drift de sérialisation. |
| 4 | `no_plaintext_secrets` | `grep` du repo ne trouve `LANEWAY_API_KEY=` qu'en placeholder dans `.env.example`. Aucune vraie clé en clair. |
| 5 | `silver_schema_contract` | `PRAGMA table_info('issues')` matche **exactement** les colonnes ET types DuckDB déclarés dans `contracts/issues.json`. Pas d'inférence pandas → DuckDB qui te rend un `DOUBLE` au lieu d'un `DECIMAL`. |

---

## Les pièges que les juniors se prennent (vus mille fois en revue)

### 1. La condition d'arrêt de pagination

```python
# ❌ Boucle infinie : l'API renvoie `null` (Python None), pas la chaîne "null".
while next_cursor != "null":
    ...

# ❌ Sort trop tôt si l'API renvoie une chaîne vide quelque part.
while next_cursor:
    ...

# ✅ Le seul cas correct.
while next_cursor is not None:
    ...
```

### 2. Retry sur 429 sans backoff

```python
# ❌ Tu vas te faire bannir / boucler quelques milliers de fois en 2 secondes.
while True:
    r = httpx.get(...)
    if r.status_code == 200:
        break

# ✅ Backoff exponentiel borné, respect du Retry-After.
for attempt in range(5):
    r = httpx.get(...)
    if r.status_code == 200:
        break
    if r.status_code in (429, 503):
        retry_after = int(r.headers.get("Retry-After", "1"))
        time.sleep(retry_after * (2 ** attempt))
        continue
    r.raise_for_status()  # autre erreur => on lève
else:
    raise RuntimeError("max retries exceeded")
```

En CI, le mock répond instantanément (`Retry-After: 1`) — pas besoin de
craindre des sleeps longs. En prod, ces sleeps **doivent** exister.

### 3. Aplatir `labels` en prenant `labels[0]`

```python
# ❌ Perte silencieuse de données.
"labels": issue["labels"][0] if issue["labels"] else None

# ✅ On concatène tout, trié, séparateur stable.
"labels": ",".join(sorted(issue["labels"])) if issue["labels"] else None
```

Le contrat veut `labels` en `VARCHAR`, pas en `LIST` ni en `VARCHAR[]`.
On *aplatit* pour le silver — l'array brut reste disponible en bronze si
quelqu'un en a besoin.

### 4. Bronze pas idempotent

```python
# ❌ Chaque run crée des fichiers différents.
fname = f"page_{int(time.time() * 1000)}.json"

# ❌ Pas atomique : une coupure réseau te laisse un JSON corrompu.
with open(path, "w") as f:
    json.dump(payload, f)

# ✅ Nom déterministe + écriture atomique via os.replace.
fname = f"page_{page_number:03d}.json"
tmp = path.with_suffix(".json.tmp")
with tmp.open("w") as f:
    json.dump(payload, f, ensure_ascii=False, sort_keys=True)
os.replace(tmp, path)
```

### 5. Silver pas idempotent

Deux stratégies acceptées par la CI :

```sql
-- Stratégie A (la plus simple, marche partout)
DELETE FROM issues WHERE ingested_date = '2026-04-10';
INSERT INTO issues SELECT ... ;

-- Stratégie B (DuckDB ≥ 0.9, marche aussi en Postgres)
INSERT INTO issues (...)
VALUES (...)
ON CONFLICT (issue_id) DO UPDATE SET
    state = EXCLUDED.state,
    closed_at = EXCLUDED.closed_at,
    ...
;
```

Choisis-en une, documente le choix dans un commentaire en haut de
`upsert_silver()`. En entretien, on te demandera pourquoi.

### 6. Inférer les types depuis pandas

`con.execute("CREATE TABLE issues AS SELECT * FROM df")` te rend une
table avec les types que DuckDB *devine*. Devine quoi ? Ce n'est pas
ce que veut le contrat. Déclare le `CREATE TABLE issues (...)` à la main
avec les types exacts du contrat, puis fais un `INSERT INTO issues SELECT
... FROM df` ou un `executemany` sur des tuples typés Python.

---

## Pour aller plus loin (références)

Aucune lecture n'est obligatoire pour valider ce projet, mais voici les
sources qui ont structuré la rubric :

- Joe Reis & Matt Housley, *Fundamentals of Data Engineering* (O'Reilly,
  2022) — **chap. 7 « Ingestion », pp. 240-255** sur les patterns API,
  l'idempotence, et les conventions bronze/silver.
- James Densmore, *Data Pipelines Pocket Reference* (O'Reilly, 2021) —
  **chap. 4 « Extracting data »** sur la pagination, le rate-limiting,
  les retry patterns.
- Martin Kleppmann, *Designing Data-Intensive Applications* (O'Reilly,
  2017) — **chap. 8 « The Trouble with Distributed Systems »** : pourquoi
  un retry doit toujours être *idempotent*, et ce que ça implique côté
  écriture (writes atomiques, dédoublonnage côté serveur, etc.).
- httpx docs : [Async retry strategies](https://www.python-httpx.org/advanced/retries/)
  et le tutoriel `httpx.HTTPTransport(retries=N)`.
- DuckDB docs : [`INSERT ... ON CONFLICT`](https://duckdb.org/docs/sql/statements/insert)
  et [Data Types](https://duckdb.org/docs/sql/data_types/overview).

---

## Si tu es bloqué

L'objectif est que tu galères un peu — la prod c'est ça. Mais si tu
tournes en rond plus d'une heure sur un check précis :

1. Relis le message d'erreur du test — il pointe presque toujours la
   cause exacte.
2. Lance le mock à la main et fais un `curl` pour comprendre les payloads :
   ```bash
   python -m mock_api.server &
   curl -H "Authorization: Bearer anything" 'http://127.0.0.1:8080/api/v1/issues?limit=3'
   ```
3. Inspecte ta table silver pour voir où ça cloche :
   ```bash
   python -c "import duckdb; con = duckdb.connect('silver.duckdb', read_only=True); \
              print(con.execute(\"PRAGMA table_info('issues')\").fetchall()); \
              print(con.execute('SELECT COUNT(*) FROM issues').fetchone())"
   ```
4. Ouvre une issue dans ton fork avec le label `help-wanted` — la
   communauté IAmDataEng y passe.

Bonne route.
