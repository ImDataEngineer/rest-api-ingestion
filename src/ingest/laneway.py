"""Laneway issues — paginated REST API → bronze (JSON) → silver (DuckDB).

Ce module est volontairement INCOMPLET. À toi de l'implémenter en suivant le
brief de `README.fr.md` et le contrat `contracts/issues.json`.

Objectif final :

    python -m src.ingest.laneway --date 2026-04-10

doit :
  1. Paginer entièrement `GET /api/v1/issues` (cursor pagination, page_size 100).
  2. Sur 429 ou 503, **retry avec backoff exponentiel** (utilise httpx Transport
     `retries=` OU une boucle manuelle — au choix). Pas de retry infini : 5
     tentatives max par requête.
  3. Stocker le JSON brut de chaque page dans
     `bronze/laneway/issues/ingested_date=YYYY-MM-DD/page_NNN.json`
     (NNN zéro-paddé sur 3 chiffres, ex. `page_001.json`).
  4. Construire la table silver `issues` dans `silver.duckdb`, conforme au
     contrat (`contracts/issues.json`), dédupliquée sur `issue_id`.
  5. Être **idempotent** : relancer `ingest_day(date)` deux fois de suite
     donne EXACTEMENT le même état (même row count silver, même fichiers
     bronze, pas de duplication).

Variables d'environnement (chargées depuis `.env` via python-dotenv) :

  LANEWAY_BASE_URL   ex. http://127.0.0.1:8765
  LANEWAY_API_KEY    n'importe quelle chaîne non-vide (le mock l'accepte)

NB : la signature publique attendue par la CI est `ingest_day(date: str) -> None`.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Iterable, Iterator

import httpx
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BRONZE_ROOT = PROJECT_ROOT / "bronze" / "laneway" / "issues"
SILVER_DB = PROJECT_ROOT / "silver.duckdb"

# Charge .env si présent. En CI, les variables sont injectées par le workflow.
load_dotenv(PROJECT_ROOT / ".env", override=False)


# ---------------------------------------------------------------------------
# 1. HTTP client : pagination + retry sur 429/503
# ---------------------------------------------------------------------------


def iter_pages(
    base_url: str,
    api_key: str,
    page_size: int = 100,
    max_retries: int = 5,
) -> Iterator[tuple[int, dict]]:
    """Yield `(page_number, json_body)` pour chaque page de l'API.

    À implémenter :
      - Appeler GET {base_url}/api/v1/issues avec `Authorization: Bearer {api_key}`.
      - Lire `next_cursor` dans la réponse pour passer à la page suivante.
      - **Arrêt correct** : `next_cursor` est `null` en JSON (=> `None` en Python),
        pas la chaîne `"null"`. Le piège classique :
            while next_cursor != "null":  # boucle infinie, l'API renvoie None
        Préfère :
            while next_cursor is not None: ...
      - Sur 429 ou 503 : retry avec backoff exponentiel. Indices :
          * httpx supporte `httpx.HTTPTransport(retries=...)` pour les erreurs
            transport, mais 429/503 sont des statuts HTTP — il faut une boucle.
          * Backoff suggéré : 0.5s, 1s, 2s, 4s, 8s (avec un peu de jitter en prod).
          * Sur 429, `Retry-After` peut indiquer combien attendre — bonus si tu
            le respectes.
      - Lever sur tout autre statut >= 400 (ne masque pas les vraies erreurs).

    NB pour les tests : la CI réinitialise les injections du mock avant chaque
    run, donc le 429 sur page 3 et le 503 sur page 5 sont reproductibles à
    l'identique. Si ton client ne retry pas, tu rateras le check
    `full_pagination` ET le check `retry_on_429_observed`.
    """
    # TODO: implémenter la boucle de pagination + retry
    raise NotImplementedError(
        "iter_pages() pas encore implémenté. Lis les indices ci-dessus."
    )


# ---------------------------------------------------------------------------
# 2. Bronze : écriture atomique des pages JSON brutes
# ---------------------------------------------------------------------------


def write_bronze_page(date: str, page_number: int, payload: dict) -> Path:
    """Écrit le JSON brut d'une page sous bronze/laneway/issues/ingested_date=<date>/page_NNN.json.

    À garantir :
      - Le chemin doit suivre EXACTEMENT le pattern Hive
        `ingested_date=YYYY-MM-DD/page_NNN.json` (NNN zéro-paddé sur 3 chiffres).
      - **Atomicité** : écris dans un fichier temporaire puis `os.replace()`
        vers le nom final. Sinon, une coupure réseau à mi-écriture te laisse
        un JSON corrompu en bronze. Le test `idempotent_replay` ne pardonne pas.
      - Le contenu est `json.dumps(payload, ensure_ascii=False, sort_keys=True)`
        avec un seul mode d'écriture (ne mélange pas indent=2 et compact selon
        les runs — sinon les bytes diffèrent entre deux replays).

    Retourne le Path final.
    """
    # TODO: créer le répertoire de partition si absent
    # TODO: écrire dans page_NNN.json.tmp puis os.replace()
    raise NotImplementedError("write_bronze_page() pas encore implémenté.")


# ---------------------------------------------------------------------------
# 3. Silver : table DuckDB typée + idempotente
# ---------------------------------------------------------------------------


def upsert_silver(issues: Iterable[dict], date: str) -> None:
    """Insère / met à jour les issues dans la table silver `issues` (silver.duckdb).

    À garantir :
      - La table existe avec les colonnes ET types EXACTS du contrat
        `contracts/issues.json` (`PRAGMA table_info('issues')` doit matcher).
      - `labels` est aplati en VARCHAR : `",".join(sorted(labels))`, ou NULL
        si la liste est vide. Surtout pas `labels[0]` (perte silencieuse).
      - `created_at` et `closed_at` parsés en TIMESTAMP. `closed_at` peut être
        NULL.
      - **Idempotence** : deux stratégies acceptées
          1) DELETE FROM issues WHERE ingested_date = <date> ; puis INSERT.
          2) INSERT ... ON CONFLICT (issue_id) DO UPDATE SET ... (DuckDB ≥ 0.9).
        Choisis-en une et justifie le choix dans un commentaire dans le code.
      - `ingested_date` est valorisé à `date` (paramètre passé) — c'est ta
        colonne de partition logique côté silver.

    NB : ne fais PAS d'insert ligne-à-ligne. Construis une liste de tuples et
    fais un seul executemany — pas pour la perf à 8000 lignes, mais pour
    garder l'INSERT atomique.
    """
    # TODO: ouvrir une connexion DuckDB, CREATE TABLE IF NOT EXISTS conforme au contrat
    # TODO: aplatir `labels`, parser les timestamps, valoriser `ingested_date`
    # TODO: stratégie d'idempotence (DELETE + INSERT, ou ON CONFLICT)
    raise NotImplementedError("upsert_silver() pas encore implémenté.")


# ---------------------------------------------------------------------------
# 4. Orchestration : ingest_day()
# ---------------------------------------------------------------------------


def ingest_day(date: str) -> None:
    """Pipeline complet pour une date donnée.

    1. Itère sur les pages de l'API.
    2. Pour chaque page : écrit le bronze (atomique) ET collecte les issues.
    3. Une fois toutes les pages reçues, upsert silver.

    Cette signature exacte (`ingest_day(date: str) -> None`) est appelée par
    la CI — ne la renomme pas, ne change pas le type de l'argument.
    """
    base_url = os.environ.get("LANEWAY_BASE_URL")
    api_key = os.environ.get("LANEWAY_API_KEY")
    if not base_url:
        raise RuntimeError(
            "LANEWAY_BASE_URL n'est pas défini. Copie .env.example en .env ou "
            "exporte la variable avant de lancer le pipeline."
        )
    if not api_key:
        raise RuntimeError(
            "LANEWAY_API_KEY n'est pas défini. Copie .env.example en .env ou "
            "exporte la variable avant de lancer le pipeline."
        )

    # TODO: appeler iter_pages(), accumuler les issues, écrire chaque page en bronze
    # TODO: appeler upsert_silver() à la fin
    # TODO: print un résumé clair (pages, retries observés, rows en silver)
    raise NotImplementedError("ingest_day() pas encore implémenté.")


# ---------------------------------------------------------------------------
# 5. CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Laneway issues for one day.")
    parser.add_argument(
        "--date",
        default=os.environ.get("INGEST_DATE", "2026-04-10"),
        help="Ingest date in YYYY-MM-DD format. Default: $INGEST_DATE or 2026-04-10.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    ingest_day(args.date)


if __name__ == "__main__":
    main()
