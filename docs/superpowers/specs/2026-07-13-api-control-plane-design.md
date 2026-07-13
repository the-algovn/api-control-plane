# api-control-plane — Design

**Date:** 2026-07-13
**Status:** Approved (brainstorm dialogue)
**Depends on:** iac `2026-07-12-kong-gateway-grpc-conventions-design.md`, iac `2026-07-13-authnz-foundation-design.md`
**Follow-up spec:** the-button (first product consumer, spec #2)

## 1. Goal

One public API host — `api.algovn.com` — for every algovn product, with a dedicated
path prefix per service. A single Go service, `api-control-plane`, sits between Kong
and the internal gRPC services. It centralizes what every product would otherwise
rebuild:

- **API registration** — which prefixes exist and where they route (GitOps, PR-reviewed).
- **AuthN/Z enforcement** — Bearer JWT (Zitadel) verification and per-route permission
  rules, from the same registration files.
- **JSON ⇄ gRPC transcoding** — internal services stay pure gRPC (existing convention);
  browsers speak HTTP/JSON.
- **Realtime push** — a shared SSE fan-out backed by RabbitMQ: any service publishes an
  event with a channel routing key; browsers subscribe via `GET /events/{channel}`.

Future (out of scope now): OpenFGA per-resource checks, runtime registration API,
per-route rate limits.

## 2. Decisions (from brainstorm Q&A)

| Question | Decision |
|---|---|
| API host | `api.algovn.com` (inside existing `*.algovn.com` cert/DNS/tunnel; `algo.vn` was shorthand). |
| Project split | Control plane is its own project + spec, built before the-button. |
| Registration | GitOps: declarative YAML per product, PR-reviewed, ConfigMap-mounted, hot-reload. No runtime registration API in v1. |
| JWT verification | Control plane verifies (JWKS from Zitadel, cached), NOT Kong's jwt-auth plugin — one host mixes anonymous and protected routes, and auth config belongs with auth enforcement. Conventions doc updated accordingly. |
| Upstream protocol | gRPC transcoding (JSON ⇄ `dynamicpb`), descriptors via gRPC server reflection. Services stay pure gRPC. |
| Realtime | SSE at the control plane, fan-out from RabbitMQ topic exchange `events`; shared mechanic for all products. |
| Frontend stack (for products) | React + Vite SPAs (recorded here; applies to the-button spec). |

## 3. Architecture

```
                    ┌────────────────────── api.algovn.com ──────────────────────┐
Browser ── Kong ──▶ │  api-control-plane                                          │
  (TLS, rate limit) │  ├─ Router: /<prefix>/… from GitOps registration files      │
                    │  ├─ Auth: JWKS(Zitadel) verify RS256+iss → rule per route   │
                    │  │        anonymous | authenticated | role:<r>              │
                    │  ├─ Transcoder: JSON ⇄ gRPC (unary), descriptors via        │
                    │  │        gRPC reflection on upstream at config load        │
                    │  └─ Push gateway: GET /events/{channel} (SSE)               │
                    └──────────┬────────────────────────────┬────────────────────┘
                               │ gRPC h2c :9090             │ consume
                               ▼                            ▼
                     internal services            RabbitMQ (topic exchange "events")
                     (pure gRPC, unchanged)                 ▲
                               └────── publish (routing key = channel) ──────┘
```

Kong's role on this host shrinks to TLS, tunnel path, and rate limiting; it routes
`api.algovn.com` to the control plane with **no** jwt-auth plugin. Other hosts
(grafana, argocd, …) keep their existing Kong gating.

Upstream services are unchanged by this design: they receive the verified
`Authorization` header as gRPC metadata and parse claims exactly per
`authnz-conventions.md` (read-only decode of segment 2, no re-verification).

## 4. Components

One Go service, repo `the-algovn/api-control-plane`.

1. **Route registry** (`internal/config`) — loads `registrations/*.yaml` from a
   ConfigMap-mounted directory; fsnotify hot-reload. Validation rejects: duplicate
   prefixes, duplicate channel names, collisions with reserved prefixes (`/events`,
   `/healthz`, `/metrics`). An invalid file is rejected whole; last good config stays
   live (log + `config_reload_errors_total`).
2. **Auth middleware** (`internal/auth`) — JWKS from Zitadel
   (`https://id.algovn.com/oauth/v2/keys`): fetched at startup with retry/backoff,
   refreshed on interval and on unknown `kid` (rate-limited). Verifies RS256 signature,
   asserts `iss == https://id.algovn.com`, evaluates the route rule:
   - `anonymous` — no token required (a present-but-invalid token is still a 401).
   - `authenticated` — valid token required.
   - `role:<r>` — valid token whose Zitadel roles claim
     (`urn:zitadel:iam:org:project:roles`) contains `<r>`.
3. **Transcoder** (`internal/transcode`, `internal/proxy`) — RPC-style mapping, zero
   per-method config: `POST /<prefix>/<fully.qualified.Service>/<Method>`, JSON body
   ⇄ `dynamicpb` ⇄ unary gRPC. Descriptors fetched via gRPC server reflection per
   upstream at startup/reload. Forwards header allowlist (`authorization`,
   `x-request-id`, `accept-language`) as metadata. Default deadline 5s, per-route
   override.
4. **Push gateway** (`internal/push`) — topic exchange `events`; routing key = channel
   name (`<product>.<topic>`, e.g. `the-button.counter`); message body = the JSON the
   browser receives, verbatim. One RabbitMQ queue per control-plane instance; binding
   added on a channel's first subscriber. In-memory hub per channel fans out to SSE
   clients. Channel auth rules come from the same registration files. No
   replay/history in v1: channels carry snapshots or fire-and-forget events; a
   reconnecting client gets the next message.
5. **Observability** — Prometheus `/metrics`: per-route request count/latency
   (bounded labels: registration route id, not raw path), SSE connection gauge,
   config reload errors. `slog` structured logs. `/healthz` liveness/readiness.

## 5. Registration schema

One YAML file per product, in this repo under `registrations/`, shipped in the
ConfigMap by kustomize:

```yaml
# registrations/the-button.yaml
prefix: /the-button
upstream: the-button-service.the-button.svc.cluster.local:9090
defaultRule: authenticated          # fallback for unlisted methods
routes:
  - method: algovn.button.v1.ButtonService/GetCounter
    rule: anonymous
  - method: algovn.button.v1.ButtonService/SubmitClicks
    rule: authenticated
    deadline: 3s
  - method: algovn.button.v1.AdminService/ResetCounter
    rule: role:admin                # Zitadel project role, from token claims
channels:
  - name: the-button.counter
    rule: anonymous
```

## 6. Data flow

### Request path

1. `POST api.algovn.com/<prefix>/<Service>/<Method>`, `Authorization: Bearer <jwt>`,
   JSON body (≤ 1 MiB).
2. Kong: TLS, rate limit, forward.
3. Control plane: longest-prefix match → registration; evaluate route rule; 401
   (missing/invalid token) or 403 (rule not satisfied) before touching the upstream.
4. Transcode JSON → `dynamicpb`, unary call with forwarded metadata and deadline.
5. Response → JSON (≤ 4 MiB). gRPC status → HTTP: `NotFound`→404,
   `PermissionDenied`→403, `InvalidArgument`→400, `Unavailable`→502,
   `DeadlineExceeded`→504, else→500. Error body: `{"code":"<grpc-code>","message":"…"}`.

### Push path

1. Service publishes JSON to exchange `events`, routing key = channel.
2. Browser opens `EventSource('https://api.algovn.com/events/<channel>')`; control
   plane checks the channel rule, binds its queue to the routing key on first
   subscriber, fans out.
3. RabbitMQ down → existing SSE connections closed cleanly, new subscriptions get 503,
   EventSource auto-reconnects with backoff. The request path is independent of
   RabbitMQ health.

### CORS

Product SPAs live on other subdomains, so the control plane handles CORS centrally:
configurable origin allowlist, v1 = `https://*.algovn.com`.

## 7. Failure modes

- **Zitadel unreachable at startup** — anonymous routes serve immediately;
  authenticated routes 503 until first JWKS fetch succeeds. Brief outages later are
  covered by the cached JWKS.
- **Upstream unreachable at config load** — reflection retried with backoff; that
  prefix returns 502 `unavailable` until descriptors resolve. Reloads keep last-known
  descriptors.
- **Slow SSE client** — per-client buffered channel; on buffer full the client is
  disconnected (EventSource reconnects).
- **Panics** — recovered in middleware → 500.

## 8. Testing

- **Unit:** prefix router; auth rules × {no token, invalid, valid, wrong role} with
  static test keys; JSON⇄dynamicpb against a golden descriptor set; config validation
  (collisions, bad YAML, reserved prefixes).
- **Integration:** in-process gRPC test server with reflection → full HTTP-JSON →
  gRPC → JSON round trip; RabbitMQ via testcontainers (podman) → publish → SSE
  receive, slow-client eviction, reconnect behavior.
- **Acceptance (in-cluster, becomes the runbook):** call `algovn.demo.v1` through
  `api.algovn.com/demo/...` with/without a real Zitadel token (expect 200/401/403);
  `rabbitmqadmin publish` → event arrives on a browser `EventSource`.

## 9. Deployment & rollout

1. `iac/platform/rabbitmq/` — single-node, Argo-managed, creds as SealedSecret
   (double-seal pattern per postgres.md: control plane consumes; publisher creds
   sealed into each product namespace).
2. This repo: Go service (`cmd/`, `internal/{config,auth,transcode,push,proxy}`,
   `registrations/`), CI from iac `templates/grpc-service` GitHub Actions
   (build + push image).
3. `iac/apps/api-control-plane/` — Deployment (1 replica), Service, Ingress
   `api.algovn.com` (class kong, no jwt-auth annotation), VMServiceScrape, ConfigMap
   from `registrations/`.
4. Register the demo service as first tenant; run acceptance checks. No demo
   workload is deployed yet (the Kong project shipped conventions only), so this step
   includes deploying a minimal `demo-service` from iac `templates/grpc-service` +
   the existing `algovn/demo/v1` proto — it stays as a permanent smoke-test tenant.
5. Docs: update iac `authnz-conventions.md` (api.algovn.com auth enforced by control
   plane, not Kong jwt-auth) + new `api-conventions.md` (registration YAML schema,
   channel naming `<product>.<topic>`, RabbitMQ publish example).

## 10. Out of scope (v2+)

OpenFGA per-resource hook · runtime registration API · per-route rate limits · event
replay / `Last-Event-ID` · streaming-RPC transcoding · multi-replica control plane ·
WebSockets.

## Appendix: first consumer requirements (the-button, spec #2)

Decisions already made for the-button that this design must support (they drove v1
scope): unlimited clicks gated by per-batch proof-of-work (client solves hashcash-style
challenge in a Web Worker; difficulty scales with batch size and server load) ·
live global counter via SSE (`the-button.counter`, anonymous) · personal + global
troll achievements · React + Vite SPA in `web/apps/the-button` · anonymous reads,
authenticated clicks (Zitadel login, SPA + PKCE).
