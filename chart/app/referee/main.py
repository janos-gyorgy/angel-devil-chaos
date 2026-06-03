"""
da-referee — Angel/Devil match referee.

Responsibilities (Phase 3):
  - Own the Postgres schema (devil_injections, angel_actions, match_results +
    their _archive twins, plus the LISTEN/NOTIFY trigger).
  - Serve the htmx UI: scoreboard, live feed, start-match button, reset button.
  - Stream live events from Postgres via SSE (LISTEN daevents).
  - /is-active: Angel polls this to know whether to run its remediation loop.
  - /angel-action: Angel POSTs detection/heal events; referee writes to DB.
  - /set-active: debug endpoint (reset-secret guarded) to toggle match_active
    manually for Phase 3 smoke testing.
  - /reset: archive + truncate the live tables.

Phase 4 will wire /start-match through to the Devil n8n webhook.

Note: each SSE client opens its own Postgres connection to LISTEN. That's fine
for the 1-2 humans watching the demo. If this ever needs to scale, refactor to a
single broadcaster task with per-client asyncio.Queue.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import psycopg
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from jinja2 import Environment, FileSystemLoader, select_autoescape
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

DATABASE_URL = os.environ.get("DATABASE_URL", "")
RESET_SECRET = os.environ.get("RESET_SECRET", "")
N8N_DEVIL_WEBHOOK = os.environ.get("N8N_DEVIL_WEBHOOK", "")  # Phase 4
N8N_TRIGGER_TOKEN = os.environ.get("N8N_TRIGGER_TOKEN", "")  # Phase 4

# In-memory match state. Set via /set-active (Phase 3 smoke test) or
# /start-match (Phase 4). The Angel polls /is-active before running its loop.
_MATCH_ACTIVE: bool = False


class AngelAction(BaseModel):
    action_id: str
    ts_detected: str
    ts_healed: str | None = None
    target: str
    fault_signature: str | None = None
    healed_by: str
    attempts: int | None = None
    tenant: str


class SetActiveRequest(BaseModel):
    active: bool


class StartMatchRequest(BaseModel):
    rounds: int = 8
    mode: str = "mixed"

env = Environment(
    loader=FileSystemLoader(Path(__file__).parent / "templates"),
    autoescape=select_autoescape(["html"]),
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS devil_injections (
  run_id           uuid PRIMARY KEY,
  match_id         uuid NOT NULL,
  round_no         int NOT NULL,
  ts               timestamptz NOT NULL DEFAULT now(),
  fault_type       text NOT NULL,
  tier             char(1) NOT NULL CHECK (tier IN ('B','C')),
  namespace        text NOT NULL,
  target           text NOT NULL,
  expected_blast_radius text,
  tenant           text NOT NULL
);
CREATE TABLE IF NOT EXISTS angel_actions (
  action_id        uuid PRIMARY KEY,
  ts_detected      timestamptz NOT NULL DEFAULT now(),
  ts_healed        timestamptz,
  target           text NOT NULL,
  fault_signature  text,
  healed_by        text NOT NULL CHECK (healed_by IN ('gitops','angel-action','escalated')),
  attempts         int,
  tenant           text NOT NULL
);
CREATE TABLE IF NOT EXISTS match_results (
  match_id         uuid PRIMARY KEY,
  started_at       timestamptz NOT NULL,
  ended_at         timestamptz NOT NULL,
  mode             text NOT NULL,
  rounds_total     int NOT NULL,
  angel_points     int NOT NULL,
  devil_points     int NOT NULL,
  devil_big_wins   int NOT NULL,
  winner           text NOT NULL CHECK (winner IN ('angel','devil')),
  notes_jsonb      jsonb
);

CREATE TABLE IF NOT EXISTS devil_injections_archive (LIKE devil_injections INCLUDING ALL);
CREATE TABLE IF NOT EXISTS angel_actions_archive    (LIKE angel_actions    INCLUDING ALL);
CREATE TABLE IF NOT EXISTS match_results_archive    (LIKE match_results    INCLUDING ALL);

CREATE OR REPLACE FUNCTION notify_daevents() RETURNS trigger AS $$
DECLARE
  payload jsonb;
BEGIN
  payload := to_jsonb(NEW) || jsonb_build_object('_table', TG_TABLE_NAME);
  PERFORM pg_notify('daevents', payload::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS devil_inj_notify  ON devil_injections;
DROP TRIGGER IF EXISTS angel_act_notify  ON angel_actions;
DROP TRIGGER IF EXISTS match_res_notify  ON match_results;

CREATE TRIGGER devil_inj_notify AFTER INSERT ON devil_injections
  FOR EACH ROW EXECUTE FUNCTION notify_daevents();
CREATE TRIGGER angel_act_notify AFTER INSERT OR UPDATE ON angel_actions
  FOR EACH ROW EXECUTE FUNCTION notify_daevents();
CREATE TRIGGER match_res_notify AFTER INSERT ON match_results
  FOR EACH ROW EXECUTE FUNCTION notify_daevents();
"""


def _init_db() -> None:
    if not DATABASE_URL:
        return
    with psycopg.connect(DATABASE_URL, connect_timeout=5, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)


@contextlib.asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    _init_db()
    yield


app = FastAPI(title="da-referee", lifespan=lifespan)


# ── routes ────────────────────────────────────────────────────────────────────


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    tpl = env.get_template("index.html")
    return HTMLResponse(tpl.render())


@app.get("/healthz")
def healthz() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/is-active")
def is_active() -> JSONResponse:
    return JSONResponse({"active": _MATCH_ACTIVE})


@app.post("/angel-action")
def angel_action(action: AngelAction) -> JSONResponse:
    if not DATABASE_URL:
        raise HTTPException(status_code=503, detail="db not configured")
    with psycopg.connect(DATABASE_URL, connect_timeout=2, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO angel_actions"
                " (action_id, ts_detected, ts_healed, target, fault_signature,"
                "  healed_by, attempts, tenant)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                " ON CONFLICT (action_id) DO UPDATE"
                "   SET ts_healed = EXCLUDED.ts_healed,"
                "       healed_by = EXCLUDED.healed_by,"
                "       attempts  = EXCLUDED.attempts",
                (
                    action.action_id,
                    action.ts_detected,
                    action.ts_healed,
                    action.target,
                    action.fault_signature,
                    action.healed_by,
                    action.attempts,
                    action.tenant,
                ),
            )
    return JSONResponse({"status": "ok", "action_id": action.action_id})


@app.post("/set-active")
def set_active(
    body: SetActiveRequest,
    x_reset_secret: str = Header(default=""),
) -> JSONResponse:
    global _MATCH_ACTIVE
    if not RESET_SECRET or x_reset_secret != RESET_SECRET:
        raise HTTPException(status_code=403, detail="bad or missing reset secret")
    _MATCH_ACTIVE = body.active
    return JSONResponse({"match_active": _MATCH_ACTIVE})


@app.get("/score")
def score() -> JSONResponse:
    if not DATABASE_URL:
        return JSONResponse({"matches": []})
    with psycopg.connect(DATABASE_URL, connect_timeout=2) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT match_id, started_at, ended_at, mode, rounds_total,"
                " angel_points, devil_points, devil_big_wins, winner"
                " FROM match_results ORDER BY started_at DESC LIMIT 20"
            )
            rows = cur.fetchall()
            cur.execute("SELECT count(*) FROM devil_injections")
            (open_injections,) = cur.fetchone()
            cur.execute(
                "SELECT count(*) FROM angel_actions WHERE ts_healed IS NULL"
            )
            (open_actions,) = cur.fetchone()
    matches = [
        {
            "match_id": str(r[0]),
            "started_at": r[1].isoformat() if r[1] else None,
            "ended_at": r[2].isoformat() if r[2] else None,
            "mode": r[3],
            "rounds_total": r[4],
            "angel_points": r[5],
            "devil_points": r[6],
            "devil_big_wins": r[7],
            "winner": r[8],
        }
        for r in rows
    ]
    return JSONResponse(
        {
            "matches": matches,
            "open_injections": open_injections,
            "open_unhealed_actions": open_actions,
        }
    )


# ── match orchestration (Phase 4) ─────────────────────────────────────────────
# The referee is the deterministic conductor: it picks the fault, writes the
# devil_injections row, dispatches to the Devil webhook (a dumb executor), then
# polls angel_actions for resolution. The Angel never reads devil_injections —
# correlation is by tenant + time window, so the decoupling invariant holds.

_TENANTS = ["acme", "globex"]

# Fault catalog. tier drives both the Angel's expected behaviour and scoring:
#   B = Argo blind spot, the Angel must act (restart) to heal it
#   C = control group, Argo selfHeal reverts the drift with no Angel action
_FAULTS: dict[str, dict[str, Any]] = {
    "db-truncate":   {"tier": "B", "mttr_target": 90, "buffer": 30},
    "scale-db-zero": {"tier": "C", "mttr_target": 60, "buffer": 30},
}


def _pick_fault(mode: str, round_no: int) -> dict[str, Any]:
    tenant = _TENANTS[(round_no - 1) % len(_TENANTS)]
    if mode == "pure-b":
        ft = "db-truncate"
    elif mode == "pure-c":
        ft = "scale-db-zero"
    else:  # mixed: ~30% Tier C (rounds 3, 7, 10 of each 10), the rest Tier B
        ft = "scale-db-zero" if (round_no % 10) in (3, 7, 0) else "db-truncate"
    spec = _FAULTS[ft]
    ns = f"da-tenant-{tenant}"
    return {
        "fault_type": ft,
        "tier": spec["tier"],
        "tenant": tenant,
        "namespace": ns,
        "target": f"mini-api.da-tenant-{tenant}",
        "mttr_target": spec["mttr_target"],
        "buffer": spec["buffer"],
    }


def _db_exec(sql: str, params: tuple = (), fetch: bool = False):
    with psycopg.connect(DATABASE_URL, connect_timeout=5, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall() if fetch else None


def _write_injection(inj: dict[str, Any], match_id: str, round_no: int) -> None:
    _db_exec(
        "INSERT INTO devil_injections"
        " (run_id, match_id, round_no, fault_type, tier, namespace, target,"
        "  expected_blast_radius, tenant)"
        " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (inj["run_id"], match_id, round_no, inj["fault_type"], inj["tier"],
         inj["namespace"], inj["target"], "single-tenant", inj["tenant"]),
    )


def _latest_heal(tenant: str, since_iso: str):
    rows = _db_exec(
        "SELECT healed_by FROM angel_actions"
        " WHERE tenant=%s AND ts_detected >= %s AND ts_healed IS NOT NULL"
        " ORDER BY ts_healed DESC LIMIT 1",
        (tenant, since_iso), fetch=True,
    )
    return rows[0][0] if rows else None


async def _run_match(match_id: str, rounds: int, mode: str) -> None:
    global _MATCH_ACTIVE
    _MATCH_ACTIVE = True
    started = datetime.now(timezone.utc)
    angel_pts = devil_pts = big_wins = 0
    notes: list[dict[str, Any]] = []
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            for rnd in range(1, rounds + 1):
                inj = _pick_fault(mode, rnd)
                inj["run_id"] = str(uuid4())
                round_start = datetime.now(timezone.utc)
                await asyncio.to_thread(_write_injection, inj, match_id, rnd)

                headers = {"x-da-trigger": N8N_TRIGGER_TOKEN} if N8N_TRIGGER_TOKEN else {}
                with contextlib.suppress(Exception):
                    # Best-effort dispatch — resolution polling decides the outcome,
                    # so a slow/failed webhook just becomes a Devil point on timeout.
                    await client.post(
                        N8N_DEVIL_WEBHOOK,
                        json={k: inj[k] for k in ("run_id", "fault_type", "tier", "tenant", "namespace")},
                        headers=headers,
                    )

                deadline = round_start.timestamp() + inj["mttr_target"] + inj["buffer"]
                healed_by = None
                while datetime.now(timezone.utc).timestamp() < deadline:
                    healed_by = await asyncio.to_thread(
                        _latest_heal, inj["tenant"], round_start.isoformat()
                    )
                    if healed_by:
                        break
                    await asyncio.sleep(3)

                mttr = (datetime.now(timezone.utc) - round_start).total_seconds()
                won = healed_by in ("gitops", "angel-action")
                if won:
                    angel_pts += 1
                else:
                    devil_pts += 1
                    if inj["tier"] == "B":
                        big_wins += 1
                notes.append({
                    "round": rnd, "fault": inj["fault_type"], "tier": inj["tier"],
                    "tenant": inj["tenant"], "healed_by": healed_by or "timeout",
                    "mttr_s": round(mttr, 1), "winner": "angel" if won else "devil",
                })

        winner = "angel" if rounds and angel_pts >= 0.75 * rounds and big_wins == 0 else "devil"
        await asyncio.to_thread(
            _db_exec,
            "INSERT INTO match_results"
            " (match_id, started_at, ended_at, mode, rounds_total, angel_points,"
            "  devil_points, devil_big_wins, winner, notes_jsonb)"
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (match_id, started, datetime.now(timezone.utc), mode, rounds,
             angel_pts, devil_pts, big_wins, winner, json.dumps(notes)),
        )
    finally:
        _MATCH_ACTIVE = False


@app.post("/start-match")
async def start_match(body: StartMatchRequest) -> JSONResponse:
    if not N8N_DEVIL_WEBHOOK:
        raise HTTPException(status_code=503, detail="N8N_DEVIL_WEBHOOK not configured")
    if _MATCH_ACTIVE:
        raise HTTPException(status_code=409, detail="a match is already running")
    if not 1 <= body.rounds <= 20:
        raise HTTPException(status_code=422, detail="rounds must be 1..20")
    match_id = str(uuid4())
    asyncio.create_task(_run_match(match_id, body.rounds, body.mode))
    return JSONResponse(
        {"status": "started", "match_id": match_id, "rounds": body.rounds, "mode": body.mode}
    )


@app.post("/reset")
def reset(x_reset_secret: str = Header(default="")) -> JSONResponse:
    if not RESET_SECRET or x_reset_secret != RESET_SECRET:
        raise HTTPException(status_code=403, detail="bad or missing reset secret")
    if not DATABASE_URL:
        raise HTTPException(status_code=503, detail="db not configured")
    with psycopg.connect(DATABASE_URL, connect_timeout=2, autocommit=True) as conn:
        with conn.cursor() as cur:
            for t in ("devil_injections", "angel_actions", "match_results"):
                cur.execute(f"INSERT INTO {t}_archive SELECT * FROM {t}")
                cur.execute(f"TRUNCATE TABLE {t}")
    return JSONResponse({"status": "reset", "ts": datetime.now(timezone.utc).isoformat()})


async def _event_stream(request: Request) -> AsyncIterator[dict[str, Any]]:
    """One Postgres LISTEN connection per client. Yields SSE events."""
    if not DATABASE_URL:
        yield {"event": "error", "data": "db not configured"}
        return

    # Initial hello so the browser sees an open stream immediately.
    yield {"event": "open", "data": "connected"}

    aconn = await psycopg.AsyncConnection.connect(DATABASE_URL, autocommit=True)
    try:
        async with aconn.cursor() as cur:
            await cur.execute("LISTEN daevents")
        gen = aconn.notifies()
        try:
            async for n in gen:
                if await request.is_disconnected():
                    break
                try:
                    payload = json.loads(n.payload)
                except Exception:
                    payload = {"raw": n.payload}
                yield {"event": payload.get("_table", "event"), "data": json.dumps(payload)}
        finally:
            with contextlib.suppress(Exception):
                await gen.aclose()
    finally:
        with contextlib.suppress(Exception):
            await aconn.close()


@app.get("/events")
async def events(request: Request) -> EventSourceResponse:
    # ping every 15s so corporate proxies / load balancers don't kill the stream
    return EventSourceResponse(_event_stream(request), ping=15)
