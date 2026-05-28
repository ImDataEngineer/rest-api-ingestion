"""Laneway mock REST API.

A small FastAPI app that simulates a paginated issue tracker:

    GET /api/v1/issues?cursor=<opaque>&limit=<int>
      → { "issues": [...], "next_cursor": "<opaque>" | null }

Quirks the learner must handle (all deterministic):

  1. Cursor pagination: the response shape uses `next_cursor`. When there is
     no more data, `next_cursor` is `null` (JSON null, NOT the string "null").
     A common pitfall: code that loops `while next_cursor != "null"` will
     never terminate.

  2. Rate limit: the FIRST request that targets `page 3` (i.e. cursor encodes
     offset == 200) returns HTTP 429 with a `Retry-After: 1` header. The
     learner's client must retry; the second call to that cursor succeeds.

  3. Transient 503: the FIRST request that targets `page 5` (offset == 400)
     returns HTTP 503. Same recovery as above — retry succeeds.

Both injections persist their state in memory so the second hit at the same
cursor is allowed through. The state is reset between test sessions via
`POST /api/v1/_admin/reset`.

Auth: any non-empty `Authorization: Bearer <token>` header is accepted. No
auth at all → HTTP 401.

This server runs:
  - Locally inside the devcontainer (port 8080 by default via docker-compose).
  - In CI as an in-process uvicorn subprocess on 127.0.0.1:8765 (see
    tests/test_evaluate.py and tests/conftest.py).
"""

from __future__ import annotations

import base64
import json

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from mock_api.data import DATASET, TOTAL_ISSUES

PAGE_LIMIT_DEFAULT = 100
PAGE_LIMIT_MAX = 100

# Map of injected-failure scenarios. The KEY is the offset that triggers it,
# the VALUE is the HTTP status to return on the FIRST hit only.
INJECTIONS = {
    200: 429,  # page 3 (offset 200 if limit=100) — rate limited once
    400: 503,  # page 5 — transient server error once
}


app = FastAPI(title="Laneway mock API", version="1.0.0")

# In-memory state. Keys are offsets that have already been hit (used to flip
# the injection off after the first failure). We also keep a counter of how
# many times each injection fired, exposed via /api/v1/_admin/injections so
# CI can assert the learner actually triggered + recovered from them.
_state: dict = {
    "served_offsets": set(),
    "injection_fires": {str(k): 0 for k in INJECTIONS},
    "total_requests": 0,
}


def _encode_cursor(offset: int) -> str:
    """Cursor is just base64(b'offset:N'). Opaque to the learner."""
    return base64.urlsafe_b64encode(f"offset:{offset}".encode()).decode()


def _decode_cursor(cursor: str | None) -> int:
    if cursor is None or cursor == "":
        return 0
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        prefix, value = raw.split(":", 1)
        if prefix != "offset":
            raise ValueError("bad prefix")
        return int(value)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid cursor")


@app.get("/api/v1/issues")
def list_issues(
    request: Request,
    cursor: str | None = Query(default=None),
    limit: int = Query(default=PAGE_LIMIT_DEFAULT, ge=1, le=PAGE_LIMIT_MAX),
    authorization: str | None = Header(default=None),
):
    _state["total_requests"] += 1

    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="empty bearer token")

    offset = _decode_cursor(cursor)
    if offset < 0 or offset > TOTAL_ISSUES:
        raise HTTPException(status_code=400, detail="cursor out of range")

    # Injected failure path — only on the FIRST hit to that exact offset.
    if offset in INJECTIONS and offset not in _state["served_offsets"]:
        status = INJECTIONS[offset]
        _state["injection_fires"][str(offset)] += 1
        _state["served_offsets"].add(offset)
        headers = {"Retry-After": "1"} if status == 429 else {}
        return JSONResponse(
            status_code=status,
            content={"error": f"injected {status}", "retry_after": 1, "offset": offset},
            headers=headers,
        )

    page = DATASET[offset : offset + limit]
    next_offset = offset + limit
    if next_offset >= TOTAL_ISSUES:
        next_cursor: str | None = None
    else:
        next_cursor = _encode_cursor(next_offset)

    # Mark the offset as served (in case the injection wasn't there, this is
    # a no-op for the gating logic but keeps the state consistent).
    _state["served_offsets"].add(offset)

    return {"issues": page, "next_cursor": next_cursor, "page_size": len(page)}


@app.get("/api/v1/_admin/injections")
def get_injections():
    """Read-only telemetry endpoint. CI uses this to confirm retries happened.

    Not part of the "real" API the learner is supposed to consume.
    """
    return {
        "injections_configured": INJECTIONS,
        "injection_fires": _state["injection_fires"],
        "total_requests": _state["total_requests"],
    }


@app.post("/api/v1/_admin/reset")
def reset_state():
    """Reset injection state so a CI run starts from a known position."""
    _state["served_offsets"] = set()
    _state["injection_fires"] = {str(k): 0 for k in INJECTIONS}
    _state["total_requests"] = 0
    return {"reset": True}


@app.get("/healthz")
def healthz():
    return {"status": "ok", "issues_loaded": len(DATASET)}


def main() -> None:
    """Run the mock API standalone (for the devcontainer / docker-compose).

    In CI we don't go through this — the test conftest launches uvicorn
    programmatically on 127.0.0.1:8765.
    """
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="warning")


if __name__ == "__main__":
    main()
