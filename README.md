# angel-devil-chaos

A small chaos-engineering **ablation harness** that answers one question with data:

> **When something breaks in a GitOps cluster, *which automation layer* actually fixes it — the kubelet, Argo CD, an agent, or nobody?**

A **Devil** injects faults. The harness then measures which layer brings the service back to green. The trick is that it measures the *residual*: it runs each fault with the agent **off** to see what the platform heals for free, and again with the agent **on** to see what's left. The agent is an **Angel** — an [n8n](https://n8n.io) workflow. The orchestrator is a small FastAPI **Referee**.

This is deliberately *not* a scoreboard that reports a number you fed it. Earlier versions did exactly that, and it was a fair criticism — see [Design notes](#design-notes-what-this-is-and-isnt).

---

## The four resolution layers

Each automation layer in a Kubernetes + GitOps stack can only see — and fix — a certain class of fault. The whole point is to draw the boundaries.

| Layer | Heals | Example fault | Mechanism |
|-------|-------|---------------|-----------|
| **kubelet** | a process that fails its probe | `crash` — `/healthz` starts failing | liveness probe restarts the pod |
| **gitops** (Argo CD) | manifest drift | `scale-db-zero`, `bad-image` — Deployment patched | selfHeal reverts live state to Git |
| **agent** (Angel) | green-but-dead state Argo can't see | `db-truncate` — table wiped, pod healthy | targeted remediation (here: restore from backup) |
| **unrecoverable** | nothing in-cluster can | `db-truncate` with no backup | needs out-of-band data or a human |

The interesting region is the **agent** row: a truncated table leaves the pod `Running`, `/healthz` at 200, and the Deployment byte-for-byte correct — so the kubelet sees nothing to restart and Argo sees no drift. A restart doesn't help either (the data is still gone). That's the gap an agent has to fill — and if there's no backup, *nobody* can, which is the honest fourth outcome.

---

## How it measures (and why that isn't circular)

For each fault the harness runs two **arms**:

- **baseline** — the Angel is paused. Only the kubelet and Argo can act.
- **agent** — the Angel is active. The residual it adds on top.

Attribution then falls out of the experiment design, no guessing required:

- recovers with the Angel **off** ⇒ the platform layer that owns it (kubelet or gitops)
- recovers **only** with the Angel on ⇒ the agent
- recovers under **neither** ⇒ unrecoverable

The output is a **resolution matrix**, not an aggregate. The number that matters — *how much the agent adds* — is the difference between the two arms, which is emergent: it depends on each fault's nature and which layer can touch it, never on how many of each fault you chose to inject.

### The matrix from a clean run

4 faults × 2 arms (`crash`, `scale-db-zero`, `bad-image`, `db-truncate`):

| fault | expected | baseline arm | agent arm |
|-------|----------|--------------|-----------|
| `crash` | kubelet | **kubelet** ✓ | **kubelet** ✓ |
| `scale-db-zero` | gitops | **gitops** ✓ | **gitops** ✓ |
| `bad-image` | gitops | **gitops** ✓ | **gitops** ✓ |
| `db-truncate` | agent | **unrecovered** | **agent** ✓ |

<!-- MATRIX -->

Read the bottom row across: a truncated table is **unrecovered** by the kubelet+Argo baseline, and only the agent arm recovers it. That single row, differing across arms, is the entire justification for an agent existing — everything above it, the platform already handles for free. (Flip `BACKUP_AVAILABLE=false` and even the agent arm goes **unrecovered** — the agent can only restore what there's a backup for.)

A real finding fell out of running this: **Argo selfHeal backs off under sustained drift.** The first drift reverts in seconds; after a burst of them, a later revert can take a couple of minutes. The gitops deadlines here are wide on purpose so that backoff isn't mis-recorded as "unrecoverable."

---

## Architecture

```
                         ┌──────────────────────────┐
                         │  Referee (FastAPI + PG)   │  da-referee ns
                         │  runs arms, polls tenant  │  /start-match {arm},
                         │  health, builds matrix    │  /score, /events (SSE)
                         └───┬───────────────┬───────┘
        POST webhook (Devil) │               │ direct health poll (/healthz,/db-ping)
                  ┌──────────┘               └─────────────┐
                  ▼                                        ▼
        ┌──────────────────┐                   ┌──────────────────────────┐
        │   Devil (n8n)    │  injects 4 faults │  Playfield tenants        │
        │  crash / scale / │ ────────────────► │  mini-api (FastAPI) +      │
        │  bad-image /      │                   │  mini-db (Postgres)        │
        │  truncate         │                   └────────────┬──────────────┘
        └──────────────────┘                                 │
                  ┌──────────────────┐         restore        │
                  │   Angel (n8n)    │ ──────────────────────►┤
                  │  gated by arm;   │   (only for truncate)  │
                  │  restore or      │                        │
                  │  stand down      │   kubelet restart ◄────┤
                  └──────────────────┘   Argo revert     ◄────┘
                                              ▲
                                      ┌───────┴────────┐
                                      │    Argo CD     │ selfHeal: true
                                      └────────────────┘
```

- **Playfield tenants** (`da-tenant-*`): a `mini-api` (FastAPI) + `mini-db` (Postgres). `mini-api` exposes the fault hooks (`/chaos/truncate`, `/chaos/crash`) and the remediation (`/admin/restore`). It seeds only on first deploy, so a restart does **not** undo a truncate — that's what makes the agent's lane real.
- **Referee** (`da-referee`): picks faults, runs arms, polls each tenant's health directly to time recovery, and attributes the resolving layer by ablation.
- **Devil / Angel** (n8n): the fault injector and the healer, each authenticating to the Kubernetes API with a separate, namespace-scoped ServiceAccount (`da-devil` can patch tenant Deployments and hit the chaos hooks; `da-angel` is narrower). Blast radius is bounded by RBAC.

---

## Design notes (what this is, and isn't)

Built honestly, including the places it could be poked:

- **The agent only earns its keep in one row.** The matrix says so plainly: the kubelet and Argo handle three of the four faults with zero agent involvement. A `livenessProbe` is the right tool for `crash`; selfHeal is the right tool for drift. The agent is justified *only* for green-but-dead corruption that survives a restart — which is exactly the row the harness isolates. Showing that boundary, rather than asserting the agent is always useful, is the point.
- **Why n8n?** It's a low-code automation platform, not a controller — it polls on a timer and has no watch API, so it's strictly worse than an operator for tight reconcile loops. It's used here because the agent's real value is *diagnosis + a targeted runbook action* (restore, escalate), where a human-readable, editable workflow is a reasonable fit, and to test how far such a platform goes for ops tasks. For `crash`/drift it correctly does nothing.
- **The faults are deliberately small.** Two of them (`crash`, `truncate`) are contrived hooks in the playfield app rather than organic failures — they exist to be unambiguous representatives of their resolution class, not to be realistic incidents.
- **Earlier this project had a self-inflicted bug** worth keeping in the open: the Angel used to poll Argo's sync status to decide whether to act, and lost the race (Argo reverts in ~5s, the Angel polled every 15s), crediting itself for GitOps's saves. The fix was to route on the *symptom* of the failing check, not on the platform's transient state — a general lesson: **a remediator that reacts to the platform's state instead of the failure's nature will race the platform.**

---

## Layout

```
chart/
  Chart.yaml  values.yaml  values-da.yml
  templates/
    playfield-tenants.yaml   mini-api + mini-db + quota + secret store, per tenant
    referee.yaml             Referee app, Postgres, IngressRoute
    n8n-sa-angel.yaml        Angel ServiceAccount + narrow healer RBAC
    n8n-sa-devil.yaml        Devil ServiceAccount + tenant-scoped fault RBAC
  app/
    mini-api/                playfield app — fault hooks + /admin/restore
    referee/                 ablation orchestrator + resolution-matrix UI
  workflows/
    devilangel-devil.json    Devil n8n workflow (4-fault injector)
    devilangel-angel.json    Angel n8n workflow (restore / stand-down healer)
```

## What this repo assumes

A **reference architecture**, not a turnkey chart — the real setup from a single-node k3s homelab, sanitized. It assumes **Argo CD** (`selfHeal: true`), **n8n** in a namespace called `n8n` (import the two workflows from `chart/workflows/` and attach a K8s bearer credential per agent), **Traefik** ingress, and **External Secrets + Vault** for the per-tenant secrets (placeholders — wire to your own). Images are built locally and imported into the node. None of these are load-bearing for the *idea*; the transferable parts are the four-layer model and the ablation method.

## Quickstart (sketch)

```bash
( cd chart/app/mini-api && docker build -t mini-api:0.3.0 . )
( cd chart/app/referee  && docker build -t da-referee:0.5.2 . )
helm template da chart -f chart/values.yaml -f chart/values-da.yml | kubectl apply -f -
# import chart/workflows/*.json into n8n, attach the da-devil / da-angel SA tokens, activate both

# run both arms, then read the matrix
curl -XPOST http://referee.example.com/start-match -H content-type:application/json -d '{"rounds":4,"arm":"baseline"}'
curl -XPOST http://referee.example.com/start-match -H content-type:application/json -d '{"rounds":4,"arm":"agent"}'
curl http://referee.example.com/score | jq .matrix
```

---

## License

MIT © Janos Gyorgy
