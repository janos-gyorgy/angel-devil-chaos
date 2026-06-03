"""
da-referee — chaos ablation harness.

The question this measures: for each fault class, *which automation layer*
resolves it — the kubelet (liveness restart), Argo CD (manifest selfHeal), an
agent (targeted remediation), or nobody (unrecoverable)?

It is not a scoreboard that echoes an injection ratio. The referee runs the
fault catalog under two arms and records, per round, the layer that actually
brought the playfield back to green:

  arm=baseline   Angel paused. Only the kubelet and Argo can act.
  arm=agent      Angel active. The residual the agent adds on top.

Resolution is attributed from the experiment design, not guessed: a fault that
recovers with the Angel paused can only have been healed by the platform layer
that owns it; a fault that recovers *only* with the Angel on is the agent's; a
fault that recovers under neither arm is unrecoverable.

Responsibilities:
  - own the Postgres schema (devil_injections, angel_actions, match_results)
  - drive the htmx UI + SSE live feed (LISTEN daevents)
  - /is-active : Angel polls this; angel_enabled is false in the baseline arm
  - /angel-action : Angel posts what it did (used for false-heal accounting)
  - /start-match : run the catalog under an arm; poll tenant health directly
  - /score : the resolution matrix
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
N8N_DEVIL_WEBHOOK = os.environ.get("N8N_DEVIL_WEBHOOK", "")
N8N_TRIGGER_TOKEN = os.environ.get("N8N_TRIGGER_TOKEN", "")

# In-memory match state. The Angel polls /is-active; it runs only when
# angel_enabled is true (false during the baseline ablation arm).
_MATCH_ACTIVE: bool = False
_ANGEL_ENABLED: bool = False
_MATCH_ARM: str = "agent"

_TENANTS = ["acme", "globex"]

# Fault catalog. expected_layer = the automation layer that *should* resolve it.
# The harness verifies this by ablation rather than trusting the label.
#   kubelet — process-level; a failing liveness probe restarts the pod
#   gitops  — manifest drift; Argo selfHeal reverts it
#   agent   — green-but-dead / data corruption; needs targeted, non-restart fix
# Deadlines are generous for the gitops faults: Argo selfHeal is event-driven
# and usually reverts drift in seconds, but it backs off under sustained drift,
# so a later round can take a couple of minutes. The deadline must outlast that
# backoff or a gitops fault would be mis-recorded as unrecoverable.
_CATALOG: list[dict[str, Any]] = [
    {"fault": "crash",         "layer": "kubelet", "target": "mini-api", "mttr_target": 75,  "buffer": 45},
    {"fault": "scale-db-zero", "layer": "gitops",  "target": "mini-db",  "mttr_target": 180, "buffer": 90},
    {"fault": "bad-image",     "layer": "gitops",  "target": "mini-db",  "mttr_target": 210, "buffer": 90},
    {"fault": "db-truncate",   "layer": "agent",   "target": "mini-api", "mttr_target": 75,  "buffer": 45},
]
_CATALOG_BY_FAULT = {c["fault"]: c for c in _CATALOG}


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
    arm: str = "agent"  # "agent" | "baseline"


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
  tier             char(1),
  namespace        text NOT NULL,
  target           text NOT NULL,
  expected_blast_radius text,
  tenant           text NOT NULL,
  expected_layer   text
);
CREATE TABLE IF NOT EXISTS angel_actions (
  action_id        uuid PRIMARY KEY,
  ts_detected      timestamptz NOT NULL DEFAULT now(),
  ts_healed        timestamptz,
  target           text NOT NULL,
  fault_signature  text,
  healed_by        text NOT NULL,
  attempts         int,
  tenant           text NOT NULL
);
CREATE TABLE IF NOT EXISTS match_results (
  match_id         uuid PRIMARY KEY,
  started_at       timestamptz NOT NULL,
  ended_at         timestamptz NOT NULL,
  mode             text NOT NULL,
  rounds_total     int NOT NULL,
  arm              text,
  resolved_jsonb   jsonb,
  notes_jsonb      jsonb
);

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

# Idempotent migration from the old scoreboard schema to the harness schema.
MIGRATE = """
ALTER TABLE devil_injections DROP CONSTRAINT IF EXISTS devil_injections_tier_check;
ALTER TABLE devil_injections ALTER COLUMN tier DROP NOT NULL;
ALTER TABLE devil_injections ADD COLUMN IF NOT EXISTS expected_layer text;
ALTER TABLE angel_actions DROP CONSTRAINT IF EXISTS angel_actions_healed_by_check;
ALTER TABLE match_results ADD COLUMN IF NOT EXISTS arm text;
ALTER TABLE match_results ADD COLUMN IF NOT EXISTS resolved_jsonb jsonb;
-- drop the old scoreboard columns; the harness records resolution, not points
ALTER TABLE match_results DROP COLUMN IF EXISTS angel_points;
ALTER TABLE match_results DROP COLUMN IF EXISTS devil_points;
ALTER TABLE match_results DROP COLUMN IF EXISTS devil_big_wins;
ALTER TABLE match_results DROP COLUMN IF EXISTS winner;
"""


def _init_db() -> None:
    if not DATABASE_URL:
        return
    with psycopg.connect(DATABASE_URL, connect_timeout=5, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)
            with contextlib.suppress(Exception):
                cur.execute(MIGRATE)


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
    # angel_enabled is the gate the Angel honours. In the baseline arm the match
    # is active (Devil injects, referee measures) but the Angel stands down.
    return JSONResponse(
        {"active": _MATCH_ACTIVE, "angel_enabled": _ANGEL_ENABLED, "arm": _MATCH_ARM}
    )


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
                    action.action_id, action.ts_detected, action.ts_healed,
                    action.target, action.fault_signature, action.healed_by,
                    action.attempts, action.tenant,
                ),
            )
    return JSONResponse({"status": "ok", "action_id": action.action_id})


@app.post("/set-active")
def set_active(body: SetActiveRequest, x_reset_secret: str = Header(default="")) -> JSONResponse:
    global _MATCH_ACTIVE, _ANGEL_ENABLED
    if not RESET_SECRET or x_reset_secret != RESET_SECRET:
        raise HTTPException(status_code=403, detail="bad or missing reset secret")
    _MATCH_ACTIVE = body.active
    _ANGEL_ENABLED = body.active
    return JSONResponse({"match_active": _MATCH_ACTIVE, "angel_enabled": _ANGEL_ENABLED})


def _db_exec(sql: str, params: tuple = (), fetch: bool = False):
    with psycopg.connect(DATABASE_URL, connect_timeout=5, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall() if fetch else None


def _pick_fault(round_no: int) -> dict[str, Any]:
    spec = _CATALOG[(round_no - 1) % len(_CATALOG)]
    tenant = _TENANTS[(round_no - 1) % len(_TENANTS)]
    return {
        "run_id": str(uuid4()),
        "fault_type": spec["fault"],
        "expected_layer": spec["layer"],
        "tenant": tenant,
        "namespace": f"da-tenant-{tenant}",
        "target": f"{spec['target']}.da-tenant-{tenant}",
        "mttr_target": spec["mttr_target"],
        "buffer": spec["buffer"],
    }


def _write_injection(inj: dict[str, Any], match_id: str, round_no: int) -> None:
    _db_exec(
        "INSERT INTO devil_injections"
        " (run_id, match_id, round_no, fault_type, tier, namespace, target,"
        "  expected_blast_radius, tenant, expected_layer)"
        " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (inj["run_id"], match_id, round_no, inj["fault_type"], None,
         inj["namespace"], inj["target"], "single-tenant", inj["tenant"],
         inj["expected_layer"]),
    )


async def _playfield_green(client: httpx.AsyncClient, ns: str) -> bool:
    """The playfield is green only if the process is live AND the DB is seeded."""
    base = f"http://mini-api.{ns}.svc.cluster.local"
    try:
        h = await client.get(f"{base}/healthz", timeout=4)
        d = await client.get(f"{base}/db-ping", timeout=4)
        return h.status_code == 200 and d.status_code == 200
    except Exception:
        return False


def _agent_claimed(tenant: str, since_iso: str) -> str | None:
    rows = _db_exec(
        "SELECT healed_by FROM angel_actions"
        " WHERE tenant=%s AND ts_detected >= %s AND healed_by='agent-action'"
        "   AND ts_healed IS NOT NULL ORDER BY ts_healed DESC LIMIT 1",
        (tenant, since_iso), fetch=True,
    )
    return rows[0][0] if rows else None


async def _ensure_steady_state(client: httpx.AsyncClient, ns: str) -> bool:
    """Reset the tenant to green before a round so damage from a prior round
    (e.g. an un-restored truncate) can't contaminate this measurement."""
    base = f"http://mini-api.{ns}.svc.cluster.local"
    for _ in range(15):
        if await _playfield_green(client, ns):
            return True
        with contextlib.suppress(Exception):
            await client.post(f"{base}/admin/restore", timeout=6)
        await asyncio.sleep(2)
    return await _playfield_green(client, ns)


async def _run_round(client: httpx.AsyncClient, inj: dict[str, Any],
                     match_id: str, round_no: int, arm: str) -> dict[str, Any]:
    ns = inj["namespace"]
    await _ensure_steady_state(client, ns)  # measure each round from green
    round_start = datetime.now(timezone.utc)
    await asyncio.to_thread(_write_injection, inj, match_id, round_no)

    headers = {"x-da-trigger": N8N_TRIGGER_TOKEN} if N8N_TRIGGER_TOKEN else {}

    async def _dispatch() -> None:
        # Best-effort; if the webhook is dropped (n8n busy) the fault never lands,
        # so phase 1 re-dispatches rather than mis-recording it as unrecovered.
        with contextlib.suppress(Exception):
            await client.post(
                N8N_DEVIL_WEBHOOK,
                json={k: inj[k] for k in ("run_id", "fault_type", "expected_layer", "tenant", "namespace")},
                headers=headers,
            )

    await _dispatch()

    # Phase 1: confirm the fault landed (playfield goes red), max ~40s, with one
    # re-dispatch if it hasn't landed by ~14s.
    landed = False
    redispatched = False
    red_deadline = round_start.timestamp() + 40
    while datetime.now(timezone.utc).timestamp() < red_deadline:
        if not await _playfield_green(client, ns):
            landed = True
            break
        if not redispatched and datetime.now(timezone.utc).timestamp() - round_start.timestamp() > 14:
            await _dispatch()
            redispatched = True
        await asyncio.sleep(2)

    # Phase 2: wait for recovery (playfield green again) up to the deadline.
    deadline = round_start.timestamp() + inj["mttr_target"] + inj["buffer"]
    recovered = False
    while datetime.now(timezone.utc).timestamp() < deadline:
        if landed and await _playfield_green(client, ns):
            recovered = True
            break
        if not landed and not await _playfield_green(client, ns):
            landed = True  # late landing
        await asyncio.sleep(3)

    mttr = (datetime.now(timezone.utc) - round_start).total_seconds()
    claimed = await asyncio.to_thread(_agent_claimed, inj["tenant"], round_start.isoformat())
    layer = inj["expected_layer"]

    # Attribution by ablation. Recovery with the Angel paused can only be the
    # platform layer that owns the fault; recovery that needed the Angel is the
    # agent's; no recovery is unrecoverable.
    if not landed:
        resolved_by = "no-fault"  # injection never took effect; not a measurement
    elif not recovered:
        resolved_by = "unrecovered"
    elif layer == "agent":
        resolved_by = "agent"  # only a targeted restore recovers a truncate
    else:
        resolved_by = layer  # kubelet or gitops did it; the agent stood down
    # False heal: the agent claimed a fault that wasn't its lane.
    false_heal = bool(claimed) and layer != "agent"

    return {
        "round": round_no, "fault": inj["fault_type"], "expected_layer": layer,
        "arm": arm, "tenant": inj["tenant"], "landed": landed,
        "recovered": recovered, "resolved_by": resolved_by,
        "false_heal": false_heal, "mttr_s": round(mttr, 1),
    }


async def _run_match(match_id: str, rounds: int, arm: str) -> None:
    global _MATCH_ACTIVE, _ANGEL_ENABLED, _MATCH_ARM
    _MATCH_ACTIVE = True
    _ANGEL_ENABLED = arm == "agent"
    _MATCH_ARM = arm
    started = datetime.now(timezone.utc)
    notes: list[dict[str, Any]] = []
    resolved: dict[str, int] = {"kubelet": 0, "gitops": 0, "agent": 0, "unrecovered": 0}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            for rnd in range(1, rounds + 1):
                inj = _pick_fault(rnd)
                note = await _run_round(client, inj, match_id, rnd, arm)
                notes.append(note)
                resolved[note["resolved_by"]] = resolved.get(note["resolved_by"], 0) + 1
        await asyncio.to_thread(
            _db_exec,
            "INSERT INTO match_results"
            " (match_id, started_at, ended_at, mode, rounds_total, arm,"
            "  resolved_jsonb, notes_jsonb)"
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (match_id, started, datetime.now(timezone.utc), "catalog", rounds,
             arm, json.dumps(resolved), json.dumps(notes)),
        )
    finally:
        _MATCH_ACTIVE = False
        _ANGEL_ENABLED = False


@app.post("/start-match")
async def start_match(body: StartMatchRequest) -> JSONResponse:
    if not N8N_DEVIL_WEBHOOK:
        raise HTTPException(status_code=503, detail="N8N_DEVIL_WEBHOOK not configured")
    if _MATCH_ACTIVE:
        raise HTTPException(status_code=409, detail="a match is already running")
    if body.arm not in ("agent", "baseline"):
        raise HTTPException(status_code=422, detail="arm must be 'agent' or 'baseline'")
    if not 1 <= body.rounds <= 40:
        raise HTTPException(status_code=422, detail="rounds must be 1..40")
    match_id = str(uuid4())
    asyncio.create_task(_run_match(match_id, body.rounds, body.arm))
    return JSONResponse(
        {"status": "started", "match_id": match_id, "rounds": body.rounds, "arm": body.arm}
    )


@app.get("/score")
def score() -> JSONResponse:
    """The resolution matrix: per (fault, arm), where did rounds get resolved."""
    if not DATABASE_URL:
        return JSONResponse({"matrix": {}, "matches": []})
    rows = _db_exec(
        "SELECT match_id, started_at, ended_at, arm, rounds_total, resolved_jsonb, notes_jsonb"
        " FROM match_results ORDER BY started_at DESC LIMIT 40", fetch=True,
    ) or []

    matrix: dict[str, dict[str, Any]] = {}
    matches = []
    for r in rows:
        match_id, started, ended, arm, rounds, resolved, notes = r
        matches.append({
            "match_id": str(match_id),
            "started_at": started.isoformat() if started else None,
            "arm": arm, "rounds_total": rounds,
            "resolved": resolved,
        })
        for n in (notes or []):
            key = f"{n['fault']}|{n.get('arm', arm)}"
            cell = matrix.setdefault(key, {
                "fault": n["fault"], "arm": n.get("arm", arm),
                "expected_layer": n.get("expected_layer"),
                "kubelet": 0, "gitops": 0, "agent": 0, "unrecovered": 0,
                "false_heals": 0, "mttr_sum": 0.0, "n": 0,
            })
            cell[n["resolved_by"]] = cell.get(n["resolved_by"], 0) + 1
            cell["false_heals"] += 1 if n.get("false_heal") else 0
            cell["mttr_sum"] += n.get("mttr_s", 0) or 0
            cell["n"] += 1

    matrix_out = []
    for cell in matrix.values():
        cell["avg_mttr_s"] = round(cell["mttr_sum"] / cell["n"], 1) if cell["n"] else None
        cell.pop("mttr_sum")
        matrix_out.append(cell)
    matrix_out.sort(key=lambda c: (c["fault"], c["arm"]))
    return JSONResponse({"matrix": matrix_out, "matches": matches})


@app.post("/reset")
def reset(x_reset_secret: str = Header(default="")) -> JSONResponse:
    if not RESET_SECRET or x_reset_secret != RESET_SECRET:
        raise HTTPException(status_code=403, detail="bad or missing reset secret")
    if not DATABASE_URL:
        raise HTTPException(status_code=503, detail="db not configured")
    with psycopg.connect(DATABASE_URL, connect_timeout=2, autocommit=True) as conn:
        with conn.cursor() as cur:
            for t in ("devil_injections", "angel_actions", "match_results"):
                cur.execute(f"TRUNCATE TABLE {t}")
    return JSONResponse({"status": "reset", "ts": datetime.now(timezone.utc).isoformat()})


async def _event_stream(request: Request) -> AsyncIterator[dict[str, Any]]:
    if not DATABASE_URL:
        yield {"event": "error", "data": "db not configured"}
        return
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
    return EventSourceResponse(_event_stream(request), ping=15)
