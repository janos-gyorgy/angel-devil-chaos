"""
mini-api — playfield app for the Angel/Devil chaos harness.

Surfaces the checks the Referee and Angel poll:
  GET  /healthz       — liveness only (the kubelet probe targets this)
  GET  /db-ping       — SELECT count from notes; "fail" if the table is empty or unreachable
  GET  /secret-check  — sha256(API_KEY)[:12]; "missing" if API_KEY is unset
  GET  /check         — HTML summary of all three (the human-readable view)

Fault injection (the Devil drives these):
  POST /chaos/truncate — wipe the notes table. NOT self-healing: a restart does
                         NOT re-seed (see _seed_db). The only in-app fix is a
                         restore, which mirrors "restore from backup".
  POST /chaos/crash    — make /healthz start failing so the kubelet liveness
                         probe restarts the pod. Clears on restart.

Remediation (the Angel calls this — it is the targeted, non-restart fix):
  POST /admin/restore  — re-seed from "backup". 503 if BACKUP_AVAILABLE is false,
                         which makes a truncate fault genuinely unrecoverable.
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
# When false, /admin/restore refuses — a truncated table becomes unrecoverable
# (no in-cluster fix; needs an out-of-band backup or a human). This is the
# honest third outcome region of the harness.
BACKUP_AVAILABLE = os.environ.get("BACKUP_AVAILABLE", "true").lower() != "false"

SEED_ROWS = [(f"seed-{i:02d}", f"observation {i}") for i in range(1, 11)]

# In-memory crash flag. Set by /chaos/crash; cleared by a process restart, which
# is exactly what the kubelet liveness probe forces. Single uvicorn worker.
_CRASHED = False


def _seed_db(force: bool = False) -> None:
    """Seed the notes table.

    On startup (force=False) we seed ONLY if the table did not already exist —
    i.e. the very first deploy. A pod that restarts after a TRUNCATE finds the
    table present-but-empty and does NOT re-seed: a restart can't heal a truncate.
    /admin/restore calls this with force=True to actually restore from "backup".
    """
    if not DATABASE_URL:
        return
    with psycopg.connect(DATABASE_URL, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.notes')")
            (existed,) = cur.fetchone()
            cur.execute(
                "CREATE TABLE IF NOT EXISTS notes ("
                " code text PRIMARY KEY, body text NOT NULL)"
            )
            if force or existed is None:
                cur.execute("TRUNCATE notes")
                cur.executemany(
                    "INSERT INTO notes (code, body) VALUES (%s, %s)"
                    " ON CONFLICT (code) DO NOTHING",
                    SEED_ROWS,
                )
        conn.commit()


@contextlib.asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    # First-deploy seed only. If mini-db isn't ready yet, k8s restarts us and the
    # next start retries — still first-deploy semantics because the table is empty.
    with contextlib.suppress(Exception):
        _seed_db(force=False)
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
                if n == 0:
                    # Seed contract is "always 10 rows". Empty == corrupted.
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
    # The kubelet liveness probe targets this. /chaos/crash flips it red so the
    # probe restarts the pod — the kubelet's lane of the resolution matrix.
    if _CRASHED:
        return JSONResponse({"status": "crashed", "tenant": TENANT}, status_code=500)
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
    """Devil fault — green-but-dead. TRUNCATE the notes table so /db-ping goes
    red while /healthz stays 200. No manifest drift (Argo blind) and NOT fixed by
    a restart (kubelet useless) — only /admin/restore brings it back."""
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


@app.post("/chaos/crash")
def chaos_crash() -> JSONResponse:
    """Devil fault — process-level. Make /healthz fail so the kubelet liveness
    probe restarts the pod. A restart clears it: this is the kubelet's lane."""
    global _CRASHED
    _CRASHED = True
    return JSONResponse({"status": "crashing", "tenant": TENANT, "detail": "/healthz now failing"})


@app.post("/admin/restore")
def admin_restore() -> JSONResponse:
    """Targeted remediation — restore the seed data ("from backup"). This is the
    Angel's lane: a fix that is neither a restart nor a manifest revert. If no
    backup is available the fault is unrecoverable and we say so."""
    if not BACKUP_AVAILABLE:
        return JSONResponse(
            {"status": "no-backup", "tenant": TENANT,
             "detail": "BACKUP_AVAILABLE=false — unrecoverable in-cluster"},
            status_code=503,
        )
    if not DATABASE_URL:
        return JSONResponse({"status": "error", "detail": "DATABASE_URL unset"}, status_code=503)
    try:
        _seed_db(force=True)
    except Exception as e:
        return JSONResponse({"status": "error", "detail": type(e).__name__}, status_code=503)
    return JSONResponse({"status": "restored", "tenant": TENANT, "detail": "seed rows restored"})


@app.get("/check", response_class=HTMLResponse)
def check() -> HTMLResponse:
    live_ok = not _CRASHED
    db_ok, db_detail = _db_ok()
    sec_ok, sec_detail = _secret_ok()
    all_ok = live_ok and db_ok and sec_ok

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
    {row("liveness", live_ok, "process up" if live_ok else "crashed")}
    {row("db-ping", db_ok, db_detail)}
    {row("secret", sec_ok, sec_detail)}
  </table>
  <div class="footer">part of the devilangel playfield — break me, watch which layer heals me</div>
</body></html>"""
    return HTMLResponse(html, status_code=200 if all_ok else 503)
