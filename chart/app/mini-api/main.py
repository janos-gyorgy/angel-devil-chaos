"""
mini-api — playfield app for the Angel/Devil chaos demo.

Surfaces three checks the Referee and Angel both poll:
  GET /healthz       — liveness only
  GET /db-ping       — SELECT 1 against mini-db; "ok" if the row comes back
  GET /secret-check  — sha256(API_KEY)[:12]; "missing" if API_KEY is unset
  GET /check         — HTML summary of all three (the human-readable view)

The endpoints are intentionally cheap; the Devil's job is to make them go red.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
from collections.abc import AsyncIterator

import psycopg
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

DATABASE_URL = os.environ.get("DATABASE_URL", "")
API_KEY = os.environ.get("API_KEY", "")
TENANT = os.environ.get("TENANT", "unknown")

SEED_ROWS = [(f"seed-{i:02d}", f"observation {i}") for i in range(1, 11)]


def _seed_db() -> None:
    """Idempotent: creates the notes table and seeds it if empty.

    This is the reset primitive — TRUNCATE + pod restart = back to known state.
    """
    if not DATABASE_URL:
        return
    with psycopg.connect(DATABASE_URL, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS notes ("
                " code text PRIMARY KEY, body text NOT NULL)"
            )
            cur.execute("SELECT count(*) FROM notes")
            (n,) = cur.fetchone()
            if n == 0:
                cur.executemany(
                    "INSERT INTO notes (code, body) VALUES (%s, %s)"
                    " ON CONFLICT (code) DO NOTHING",
                    SEED_ROWS,
                )
            conn.commit()


@contextlib.asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    # Best-effort seed on startup. If mini-db isn't ready yet, k8s will restart us.
    _seed_db()
    yield


app = FastAPI(title=f"mini-api ({TENANT})", lifespan=lifespan)


def _db_ok() -> tuple[bool, str]:
    if not DATABASE_URL:
        return False, "DATABASE_URL unset"
    try:
        with psycopg.connect(DATABASE_URL, connect_timeout=2) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM notes")
                (n,) = cur.fetchone()
                # Treat an empty notes table as corruption: the seed contract is
                # "always 10 rows". Devil's TRUNCATE fault becomes visible here.
                if n == 0:
                    return False, "0 notes (corrupted/wiped)"
                return True, f"{n} notes"
    except Exception as e:
        return False, type(e).__name__


def _secret_ok() -> tuple[bool, str]:
    if not API_KEY:
        return False, "missing"
    digest = hashlib.sha256(API_KEY.encode()).hexdigest()[:12]
    return True, f"sha256:{digest}"


@app.get("/healthz")
def healthz() -> JSONResponse:
    return JSONResponse({"status": "ok", "tenant": TENANT})


@app.get("/db-ping")
def db_ping() -> JSONResponse:
    ok, detail = _db_ok()
    return JSONResponse(
        {"status": "ok" if ok else "fail", "detail": detail},
        status_code=200 if ok else 503,
    )


@app.get("/secret-check")
def secret_check() -> JSONResponse:
    ok, detail = _secret_ok()
    return JSONResponse(
        {"status": "ok" if ok else "fail", "detail": detail},
        status_code=200 if ok else 503,
    )


@app.post("/chaos/truncate")
def chaos_truncate() -> JSONResponse:
    """Devil's fault injection — the app corrupts its own state.

    This is the "green-but-dead" primitive: TRUNCATE the notes table so /db-ping
    goes red while /healthz keeps returning 200. The pod stays Running, the
    Deployment manifest is unchanged, so Argo sees nothing to heal — only a
    targeted restart (which re-runs the startup reseed) brings it back. Reachable
    in-cluster only; the tenant NetworkPolicy gates who can call it.
    """
    if not DATABASE_URL:
        return JSONResponse({"status": "error", "detail": "DATABASE_URL unset"}, status_code=503)
    try:
        with psycopg.connect(DATABASE_URL, connect_timeout=2) as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE notes")
            conn.commit()
    except Exception as e:
        return JSONResponse({"status": "error", "detail": type(e).__name__}, status_code=503)
    return JSONResponse({"status": "corrupted", "tenant": TENANT, "detail": "notes truncated"})


@app.get("/check", response_class=HTMLResponse)
def check() -> HTMLResponse:
    db_ok, db_detail = _db_ok()
    sec_ok, sec_detail = _secret_ok()
    all_ok = db_ok and sec_ok

    def row(label: str, ok: bool, detail: str) -> str:
        color = "#1ca850" if ok else "#c0392b"
        glyph = "OK" if ok else "FAIL"
        return (
            f'<tr><td>{label}</td>'
            f'<td style="color:{color};font-weight:600">{glyph}</td>'
            f'<td><code>{detail}</code></td></tr>'
        )

    banner_color = "#1ca850" if all_ok else "#c0392b"
    banner_text = "PLAYFIELD GREEN" if all_ok else "PLAYFIELD RED"

    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>{TENANT} — playfield</title>
<style>
  body {{ font-family: ui-sans-serif, system-ui, sans-serif; max-width: 540px;
         margin: 3rem auto; padding: 0 1rem; color: #222; }}
  .banner {{ padding: 1rem; border-radius: 6px; color: white;
             font-weight: 700; letter-spacing: 0.05em; text-align: center;
             background: {banner_color}; margin-bottom: 1.5rem; }}
  table {{ width: 100%; border-collapse: collapse; }}
  td {{ padding: 0.5rem 0.25rem; border-bottom: 1px solid #eee; }}
  td:first-child {{ width: 7rem; }}
  td:nth-child(2) {{ width: 4rem; }}
  code {{ background: #f4f4f4; padding: 0.1rem 0.3rem; border-radius: 3px; }}
  .footer {{ margin-top: 1.5rem; color: #888; font-size: 0.85rem; }}
</style></head>
<body>
  <div class="banner">{banner_text}</div>
  <h2>tenant: {TENANT}</h2>
  <table>
    <tr><td><b>check</b></td><td><b>state</b></td><td><b>detail</b></td></tr>
    {row("liveness", True, "process up")}
    {row("db-ping", db_ok, db_detail)}
    {row("secret", sec_ok, sec_detail)}
  </table>
  <div class="footer">part of the devilangel playfield — break me, watch me heal</div>
</body></html>"""
    return HTMLResponse(html, status_code=200 if all_ok else 503)
