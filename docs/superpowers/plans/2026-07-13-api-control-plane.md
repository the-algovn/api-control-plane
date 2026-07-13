# api-control-plane v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `api.algovn.com`: a Go gateway service that routes path-prefixed HTTP/JSON requests to internal gRPC services (transcoding via reflection), enforces Zitadel JWT auth per route, and fans out RabbitMQ events to browsers over SSE — plus the RabbitMQ platform component, a demo-service smoke tenant, and conventions docs.

**Architecture:** Kong routes the whole `api.algovn.com` host (no jwt-auth plugin) to `api-control-plane`, which loads GitOps-declared registration YAML, verifies Bearer JWTs against Zitadel's JWKS, transcodes `POST /<prefix>/<Service>/<Method>` JSON to unary gRPC using descriptors fetched via server reflection, and serves `GET /events/{channel}` SSE backed by a RabbitMQ topic exchange `events`. Spec: `docs/superpowers/specs/2026-07-13-api-control-plane-design.md`.

**Tech Stack:** Go 1.26, grpc-go + protobuf (`dynamicpb`/`protojson`), `jhump/protoreflect` (reflection client), `golang-jwt/jwt/v5` + `MicahParks/keyfunc/v3` (JWKS), `rabbitmq/amqp091-go`, `fsnotify`, `prometheus/client_golang`, testify, testcontainers-go (integration), buf (test proto), kustomize + Argo CD (deploy).

## Global Constraints

- Go `1.26.4`; module `github.com/the-algovn/api-control-plane`; repo `/Users/duclm27/the-algovn/api-control-plane` (exists, has spec commit).
- Working copies: iac at `/Users/duclm27/the-algovn/iac`, protos at `/Users/duclm27/the-algovn/protos`. Both GitHub repos are **public** (no auth for `go get`).
- Listeners: HTTP `:8080` (public via Kong), metrics `:9091` (cluster-internal only — never exposed through the Ingress). Upstream gRPC is h2c `:9090` (existing convention).
- Issuer `https://id.algovn.com`; JWKS URL `https://id.algovn.com/oauth/v2/keys`; roles claim `urn:zitadel:iam:org:project:roles`.
- Auth rules: `anonymous` | `authenticated` | `role:<r>` (`<r>` matches `[a-z0-9_-]+`). A present-but-invalid token is 401 even on `anonymous` routes.
- Limits: request body 1 MiB, response 4 MiB, default upstream deadline 5s. Forwarded headers (HTTP→gRPC metadata): `authorization`, `x-request-id`, `accept-language` only.
- gRPC→HTTP status map (spec §6): NotFound→404, PermissionDenied→403, InvalidArgument→400, Unavailable→502, DeadlineExceeded→504, Unauthenticated→401, else→500. Error body always `{"code":"<grpc-code>","message":"…"}`.
- Reserved prefixes: `/events`, `/healthz`, `/metrics`. Registration prefixes: single lowercase segment `^/[a-z0-9-]+$`. Channel names: `^[a-z0-9-]+(\.[a-z0-9-]+)+$` (`<product>.<topic>`).
- RabbitMQ: topic exchange `events`, durable; message body = verbatim browser JSON.
- Image `ghcr.io/the-algovn/api-control-plane`, **linux/amd64 only** (runs on `algovn-w1`; matches CI template). GHCR package must be set public after first push (no pull secret).
- iac: no plaintext secrets — SealedSecrets via local `kubeseal --context algovn-remote` (never ssh); run `scripts/validate.sh` before every iac push; kubectl uses context `algovn-remote`.
- k8s YAML style: compact flow maps (`{ }`) as in existing iac manifests.
- Commits: small and focused, imperative subject, **no** Co-Authored-By/Generated-with trailers. Stage explicit files only.
- Tests: testify `require`; unit tests must not need network/cluster; integration tests build-tagged `//go:build integration`.

## File Structure

```
api-control-plane repo:
  go.mod / go.sum / .gitignore
  cmd/api-control-plane/main.go        wiring: env → components → serve
  cmd/demo-service/main.go             smoke tenant: DemoService impl + health + reflection + optional AMQP publish
  internal/config/config.go            types, LoadDir, validation, Snapshot lookups
  internal/config/config_test.go
  internal/config/store.go             Store: atomic snapshot + fsnotify Watch
  internal/config/store_test.go
  internal/auth/verifier.go            JWKS keyfunc lifecycle, Verify → Identity
  internal/auth/rules.go               Authorize(rule, header) → Identity | *AuthError
  internal/auth/auth_test.go
  internal/auth/authtest/authtest.go   test JWKS server + token signer (exported helper)
  internal/testsvc/{buf.yaml,buf.gen.yaml,test.proto}   test fixture proto
  internal/testsvc/gen/…               committed generated code
  internal/testsvc/server.go           StartServer(t) → addr (Echo/Fail/Slow + reflection)
  internal/transcode/registry.go       Registry.Reconcile (dial+reflect), Backend method cache
  internal/transcode/invoke.go         Backend.Invoke: JSON⇄dynamicpb, deadline, metadata
  internal/transcode/transcode_test.go
  internal/push/hub.go                 Hub: subscribe/publish/fan-out, binding hooks
  internal/push/hub_test.go
  internal/push/consumer.go            RabbitMQ consume loop, reconnect, bind/unbind
  internal/push/consumer_integration_test.go   (tag: integration)
  internal/observability/metrics.go    prometheus vectors
  internal/httpserver/server.go        Handler(): routing, SSE endpoint, transcode endpoint
  internal/httpserver/middleware.go    recovery, access log, CORS, limits, metrics
  internal/httpserver/server_test.go   e2e: httptest + testsvc + authtest
  Dockerfile
  .github/workflows/build.yaml

protos repo:            algovn/demo/v1/demo.proto (+WhoAmI, +AdminPing), regenerated gen/go, tag gen/go/v0.1.0
iac repo:
  platform/rabbitmq/manifests/…        ConfigMap, StatefulSet, Service, SealedSecret, VMServiceScrape
  clusters/algovn/platform/rabbitmq.yaml
  apps/api-control-plane/…             ns, deployment, service, ingress, sealed amqp-creds, VMServiceScrape,
                                       registrations/demo.yaml (configMapGenerator), kustomization
  apps/demo-service/…                  grpc-service template instance (+ sealed amqp-creds)
  clusters/algovn/apps/{api-control-plane,demo-service}.yaml
  platform/image-updater/{api-control-plane-updater,demo-service-updater}.yaml
  docs/api-conventions.md              NEW: registration + publish conventions
  docs/authnz-conventions.md           EDIT: api.algovn.com gate = control plane
  docs/runbooks/api-control-plane.md   NEW: acceptance checks
```

Task order: 1 (RabbitMQ platform) is independent — everything else is sequential-ish: 2→3→4→5→6→7→8→9 (service), 10 (protos+binaries+CI), 11 (iac deploy), 12 (acceptance+docs).

---

### Task 1: RabbitMQ platform component (iac)

**Files:**
- Create: `iac/platform/rabbitmq/manifests/kustomization.yaml`
- Create: `iac/platform/rabbitmq/manifests/plugins-configmap.yaml`
- Create: `iac/platform/rabbitmq/manifests/default-user-sealed.yaml` (generated by kubeseal)
- Create: `iac/platform/rabbitmq/manifests/statefulset.yaml`
- Create: `iac/platform/rabbitmq/manifests/service.yaml`
- Create: `iac/platform/rabbitmq/manifests/vmservicescrape.yaml`
- Create: `iac/clusters/algovn/platform/rabbitmq.yaml`

**Interfaces:**
- Produces: AMQP endpoint `rabbitmq.rabbitmq.svc.cluster.local:5672`, vhost `/`, user `events` (password in password manager as `rabbitmq-events`). Tasks 11–12 consume the same password when sealing `amqp-creds` for the app namespaces.

- [ ] **Step 1: Generate the password and sealed default-user secret**

```bash
cd /Users/duclm27/the-algovn/iac
RABBIT_PASS=$(openssl rand -base64 24 | tr -d '/+=' | head -c 32)
echo "SAVE THIS to the password manager as 'rabbitmq-events': $RABBIT_PASS"
mkdir -p platform/rabbitmq/manifests
kubectl create secret generic rabbitmq-default-user -n rabbitmq \
  --from-literal=username=events --from-literal=password="$RABBIT_PASS" \
  --dry-run=client -o yaml \
  | kubeseal --context algovn-remote --format yaml \
  > platform/rabbitmq/manifests/default-user-sealed.yaml
```

Expected: file starts with `apiVersion: bitnami.com/v1alpha1` / `kind: SealedSecret`. Keep `$RABBIT_PASS` in the shell — Task 11 Step 2 needs it (if the shell is lost, read it back from the password manager).

> The default user is created by RabbitMQ only on FIRST boot (empty data dir). Password changes later require `rabbitmqctl change_password` — note this in the runbook (Task 12).

- [ ] **Step 2: Write the manifests**

`platform/rabbitmq/manifests/plugins-configmap.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: rabbitmq-plugins
  namespace: rabbitmq
data:
  enabled_plugins: |
    [rabbitmq_management,rabbitmq_prometheus].
```

`platform/rabbitmq/manifests/statefulset.yaml`:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: rabbitmq
  namespace: rabbitmq
spec:
  serviceName: rabbitmq
  replicas: 1
  selector:
    matchLabels: { app: rabbitmq }
  template:
    metadata:
      labels: { app: rabbitmq }
    spec:
      containers:
        - name: rabbitmq
          image: docker.io/rabbitmq:4.1-management-alpine
          ports:
            - { containerPort: 5672, name: amqp }
            - { containerPort: 15672, name: management }
            - { containerPort: 15692, name: prometheus }
          env:
            - name: RABBITMQ_DEFAULT_USER
              valueFrom: { secretKeyRef: { name: rabbitmq-default-user, key: username } }
            - name: RABBITMQ_DEFAULT_PASS
              valueFrom: { secretKeyRef: { name: rabbitmq-default-user, key: password } }
          readinessProbe:
            exec: { command: [rabbitmq-diagnostics, -q, ping] }
            initialDelaySeconds: 10
            periodSeconds: 30
            timeoutSeconds: 10
          livenessProbe:
            exec: { command: [rabbitmq-diagnostics, -q, status] }
            initialDelaySeconds: 60
            periodSeconds: 60
            timeoutSeconds: 15
          resources:
            requests: { cpu: 100m, memory: 256Mi }
            limits: { memory: 512Mi }
          volumeMounts:
            - { name: data, mountPath: /var/lib/rabbitmq }
            - { name: plugins, mountPath: /etc/rabbitmq/enabled_plugins, subPath: enabled_plugins }
      volumes:
        - name: plugins
          configMap: { name: rabbitmq-plugins }
  volumeClaimTemplates:
    - metadata: { name: data }
      spec:
        accessModes: [ReadWriteOnce]
        storageClassName: local-path
        resources: { requests: { storage: 2Gi } }
```

`platform/rabbitmq/manifests/service.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: rabbitmq
  namespace: rabbitmq
  labels: { app: rabbitmq }   # VMServiceScrape selects Services by THEIR labels
spec:
  selector: { app: rabbitmq }
  ports:
    - { port: 5672, targetPort: 5672, name: amqp }
    - { port: 15672, targetPort: 15672, name: management }
    - { port: 15692, targetPort: 15692, name: prometheus }
```

`platform/rabbitmq/manifests/vmservicescrape.yaml`:

```yaml
apiVersion: operator.victoriametrics.com/v1beta1
kind: VMServiceScrape
metadata:
  name: rabbitmq
  namespace: monitoring
spec:
  namespaceSelector: { matchNames: [rabbitmq] }
  selector:
    matchLabels: { app: rabbitmq }
  endpoints:
    - port: prometheus
```

`platform/rabbitmq/manifests/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - plugins-configmap.yaml
  - default-user-sealed.yaml
  - statefulset.yaml
  - service.yaml
  - vmservicescrape.yaml
```

`clusters/algovn/platform/rabbitmq.yaml`:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: rabbitmq
  namespace: argocd
  annotations:
    argocd.argoproj.io/sync-wave: "1"
spec:
  project: default
  source:
    repoURL: https://github.com/the-algovn/iac.git
    targetRevision: main
    path: platform/rabbitmq/manifests
  destination:
    server: https://kubernetes.default.svc
    namespace: rabbitmq
  syncPolicy:
    automated: { prune: true, selfHeal: true }
    syncOptions: [CreateNamespace=true, ServerSideApply=true]
    retry:
      limit: 5
      backoff: { duration: 30s, factor: 2, maxDuration: 5m }
```

- [ ] **Step 3: Validate and push**

```bash
cd /Users/duclm27/the-algovn/iac
./scripts/validate.sh
git add platform/rabbitmq clusters/algovn/platform/rabbitmq.yaml
git commit -m "Add RabbitMQ platform component (events bus for api-control-plane)"
git push
```

Expected: validate.sh exits 0; push succeeds.

- [ ] **Step 4: Verify Argo sync and broker health**

```bash
kubectl --context algovn-remote -n rabbitmq rollout status statefulset/rabbitmq --timeout=300s
kubectl --context algovn-remote -n rabbitmq exec rabbitmq-0 -- rabbitmq-diagnostics -q ping
kubectl --context algovn-remote -n rabbitmq exec rabbitmq-0 -- rabbitmqctl list_users
```

Expected: `Ping succeeded`; user list contains `events`. (If Argo hasn't picked up the new Application within ~3 min: `argocd app sync rabbitmq --core`.)

---

### Task 2: Repo scaffold + registration config loader

**Files:**
- Create: `go.mod`, `.gitignore`
- Create: `internal/config/config.go`
- Test: `internal/config/config_test.go`

**Interfaces:**
- Produces (package `config`):
  - `const DefaultDeadline = 5 * time.Second`
  - `type Duration time.Duration` (YAML `"3s"` strings)
  - `type Route struct { Method string; Rule string; Deadline Duration }`
  - `type Channel struct { Name, Rule string }`
  - `type Registration struct { Prefix, Upstream, DefaultRule string; Routes []Route; Channels []Channel }` with method `RouteRule(method string) (rule string, deadline time.Duration)`
  - `func LoadDir(dir string) (*Snapshot, error)` — parses+validates ALL `*.yaml`; any invalid file fails the whole load
  - `type Snapshot` with `Match(path string) (*Registration, bool)`, `ChannelRule(name string) (string, bool)`, `Registrations() []*Registration`
  - `func ValidRule(rule string) bool`

- [ ] **Step 1: Scaffold the module**

```bash
cd /Users/duclm27/the-algovn/api-control-plane
go mod init github.com/the-algovn/api-control-plane
printf '%s\n' 'bin/' '*.test' '.DS_Store' > .gitignore
go get gopkg.in/yaml.v3 github.com/stretchr/testify
```

- [ ] **Step 2: Write the failing test**

`internal/config/config_test.go`:

```go
package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func writeReg(t *testing.T, dir, name, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644))
}

const demoReg = `
prefix: /demo
upstream: dns:///demo-service.demo-service.svc.cluster.local:9090
defaultRule: authenticated
routes:
  - method: algovn.demo.v1.DemoService/Ping
    rule: anonymous
  - method: algovn.demo.v1.DemoService/AdminPing
    rule: role:admin
    deadline: 3s
channels:
  - name: demo.ping
    rule: anonymous
`

func TestLoadDir_Valid(t *testing.T) {
	dir := t.TempDir()
	writeReg(t, dir, "demo.yaml", demoReg)

	snap, err := LoadDir(dir)
	require.NoError(t, err)

	reg, ok := snap.Match("/demo/algovn.demo.v1.DemoService/Ping")
	require.True(t, ok)
	require.Equal(t, "/demo", reg.Prefix)
	require.Equal(t, "dns:///demo-service.demo-service.svc.cluster.local:9090", reg.Upstream)

	rule, deadline := reg.RouteRule("algovn.demo.v1.DemoService/Ping")
	require.Equal(t, "anonymous", rule)
	require.Equal(t, DefaultDeadline, deadline)

	rule, deadline = reg.RouteRule("algovn.demo.v1.DemoService/AdminPing")
	require.Equal(t, "role:admin", rule)
	require.Equal(t, 3*time.Second, deadline)

	// unlisted method falls back to defaultRule
	rule, deadline = reg.RouteRule("algovn.demo.v1.DemoService/Other")
	require.Equal(t, "authenticated", rule)
	require.Equal(t, DefaultDeadline, deadline)

	cr, ok := snap.ChannelRule("demo.ping")
	require.True(t, ok)
	require.Equal(t, "anonymous", cr)
	_, ok = snap.ChannelRule("nope.nope")
	require.False(t, ok)

	_, ok = snap.Match("/unknown/x/y")
	require.False(t, ok)
}

func TestLoadDir_UpstreamSchemeNormalized(t *testing.T) {
	dir := t.TempDir()
	writeReg(t, dir, "a.yaml", "prefix: /a\nupstream: svc.ns.svc:9090\n")
	snap, err := LoadDir(dir)
	require.NoError(t, err)
	reg, _ := snap.Match("/a/x/y")
	require.Equal(t, "dns:///svc.ns.svc:9090", reg.Upstream)
	// empty defaultRule defaults to authenticated
	rule, _ := reg.RouteRule("x/y")
	require.Equal(t, "authenticated", rule)
}

func TestLoadDir_Invalid(t *testing.T) {
	cases := map[string]string{
		"reserved prefix":   "prefix: /events\nupstream: s:9090\n",
		"bad prefix":        "prefix: /Two/Seg\nupstream: s:9090\n",
		"missing upstream":  "prefix: /a\n",
		"bad rule":          "prefix: /a\nupstream: s:9090\ndefaultRule: sometimes\n",
		"bad role rule":     "prefix: /a\nupstream: s:9090\nroutes: [{method: p.S/M, rule: 'role:'}]\n",
		"bad channel name":  "prefix: /a\nupstream: s:9090\nchannels: [{name: nodot, rule: anonymous}]\n",
		"bad yaml":          "prefix: [unclosed\n",
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeReg(t, dir, "bad.yaml", content)
			_, err := LoadDir(dir)
			require.Error(t, err)
		})
	}
}

func TestLoadDir_CrossFileCollisions(t *testing.T) {
	dir := t.TempDir()
	writeReg(t, dir, "a.yaml", "prefix: /a\nupstream: s1:9090\nchannels: [{name: a.x, rule: anonymous}]\n")
	writeReg(t, dir, "b.yaml", "prefix: /a\nupstream: s2:9090\n")
	_, err := LoadDir(dir)
	require.ErrorContains(t, err, "duplicate prefix")

	dir2 := t.TempDir()
	writeReg(t, dir2, "a.yaml", "prefix: /a\nupstream: s1:9090\nchannels: [{name: a.x, rule: anonymous}]\n")
	writeReg(t, dir2, "b.yaml", "prefix: /b\nupstream: s2:9090\nchannels: [{name: a.x, rule: anonymous}]\n")
	_, err = LoadDir(dir2)
	require.ErrorContains(t, err, "duplicate channel")
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/config/ -v`
Expected: FAIL — compile errors (`LoadDir` undefined).

- [ ] **Step 4: Implement `internal/config/config.go`**

```go
// Package config loads and validates API registration files.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const DefaultDeadline = 5 * time.Second

var (
	reservedPrefixes = map[string]bool{"/events": true, "/healthz": true, "/metrics": true}
	prefixRe         = regexp.MustCompile(`^/[a-z0-9-]+$`)
	channelRe        = regexp.MustCompile(`^[a-z0-9-]+(\.[a-z0-9-]+)+$`)
	roleRe           = regexp.MustCompile(`^role:[a-z0-9_-]+$`)
)

// Duration parses YAML strings like "3s" via time.ParseDuration.
type Duration time.Duration

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

type Route struct {
	Method   string   `yaml:"method"`
	Rule     string   `yaml:"rule"`
	Deadline Duration `yaml:"deadline"`
}

type Channel struct {
	Name string `yaml:"name"`
	Rule string `yaml:"rule"`
}

type Registration struct {
	Prefix      string    `yaml:"prefix"`
	Upstream    string    `yaml:"upstream"`
	DefaultRule string    `yaml:"defaultRule"`
	Routes      []Route   `yaml:"routes"`
	Channels    []Channel `yaml:"channels"`

	routeIdx map[string]Route
}

// RouteRule returns the auth rule and upstream deadline for a
// "pkg.Service/Method" string, falling back to DefaultRule.
func (r *Registration) RouteRule(method string) (string, time.Duration) {
	if rt, ok := r.routeIdx[method]; ok {
		d := time.Duration(rt.Deadline)
		if d == 0 {
			d = DefaultDeadline
		}
		return rt.Rule, d
	}
	return r.DefaultRule, DefaultDeadline
}

func ValidRule(rule string) bool {
	return rule == "anonymous" || rule == "authenticated" || roleRe.MatchString(rule)
}

type Snapshot struct {
	regs     map[string]*Registration // key: prefix
	channels map[string]string        // channel name -> rule
}

// Match resolves the registration owning a request path by its first segment.
func (s *Snapshot) Match(path string) (*Registration, bool) {
	rest := strings.TrimPrefix(path, "/")
	seg, _, _ := strings.Cut(rest, "/")
	reg, ok := s.regs["/"+seg]
	return reg, ok
}

func (s *Snapshot) ChannelRule(name string) (string, bool) {
	rule, ok := s.channels[name]
	return rule, ok
}

func (s *Snapshot) Registrations() []*Registration {
	out := make([]*Registration, 0, len(s.regs))
	for _, r := range s.regs {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Prefix < out[j].Prefix })
	return out
}

// LoadDir parses and validates every *.yaml file in dir. Any invalid file
// fails the whole load — the caller keeps its previous snapshot.
func LoadDir(dir string) (*Snapshot, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	snap := &Snapshot{regs: map[string]*Registration{}, channels: map[string]string{}}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		var reg Registration
		dec := yaml.NewDecoder(strings.NewReader(string(data)))
		dec.KnownFields(true)
		if err := dec.Decode(&reg); err != nil {
			return nil, fmt.Errorf("%s: %w", e.Name(), err)
		}
		if err := validate(&reg); err != nil {
			return nil, fmt.Errorf("%s: %w", e.Name(), err)
		}
		if _, dup := snap.regs[reg.Prefix]; dup {
			return nil, fmt.Errorf("%s: duplicate prefix %s", e.Name(), reg.Prefix)
		}
		snap.regs[reg.Prefix] = &reg
		for _, ch := range reg.Channels {
			if _, dup := snap.channels[ch.Name]; dup {
				return nil, fmt.Errorf("%s: duplicate channel %s", e.Name(), ch.Name)
			}
			snap.channels[ch.Name] = ch.Rule
		}
	}
	return snap, nil
}

func validate(r *Registration) error {
	if !prefixRe.MatchString(r.Prefix) {
		return fmt.Errorf("prefix %q must match %s", r.Prefix, prefixRe)
	}
	if reservedPrefixes[r.Prefix] {
		return fmt.Errorf("prefix %q is reserved", r.Prefix)
	}
	if r.Upstream == "" {
		return fmt.Errorf("upstream is required")
	}
	if !strings.Contains(r.Upstream, "://") && !strings.HasPrefix(r.Upstream, "dns:///") {
		r.Upstream = "dns:///" + r.Upstream
	}
	if r.DefaultRule == "" {
		r.DefaultRule = "authenticated"
	}
	if !ValidRule(r.DefaultRule) {
		return fmt.Errorf("invalid defaultRule %q", r.DefaultRule)
	}
	r.routeIdx = make(map[string]Route, len(r.Routes))
	for _, rt := range r.Routes {
		if rt.Method == "" || !strings.Contains(rt.Method, "/") {
			return fmt.Errorf("route method %q must be pkg.Service/Method", rt.Method)
		}
		if !ValidRule(rt.Rule) {
			return fmt.Errorf("route %s: invalid rule %q", rt.Method, rt.Rule)
		}
		if _, dup := r.routeIdx[rt.Method]; dup {
			return fmt.Errorf("duplicate route %s", rt.Method)
		}
		r.routeIdx[rt.Method] = rt
	}
	for _, ch := range r.Channels {
		if !channelRe.MatchString(ch.Name) {
			return fmt.Errorf("channel %q must match <product>.<topic>", ch.Name)
		}
		if !ValidRule(ch.Rule) {
			return fmt.Errorf("channel %s: invalid rule %q", ch.Name, ch.Rule)
		}
	}
	return nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/config/ -v`
Expected: PASS (all subtests).

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum .gitignore internal/config/config.go internal/config/config_test.go
git commit -m "Add registration config loader with validation"
```

---

### Task 3: Config store with fsnotify hot reload

**Files:**
- Create: `internal/config/store.go`
- Test: `internal/config/store_test.go`

**Interfaces:**
- Consumes: `LoadDir`, `Snapshot` (Task 2).
- Produces:
  - `type Store struct { OnReloadError func(error) }` (exported field, optional)
  - `func NewStore(dir string) (*Store, error)` — initial LoadDir must succeed
  - `func (s *Store) Get() *Snapshot`
  - `func (s *Store) Watch(ctx context.Context, logger *slog.Logger)` — blocks until ctx cancelled; reloads on fs events (200ms debounce); on reload error keeps last good snapshot and calls OnReloadError

- [ ] **Step 1: Write the failing test**

`internal/config/store_test.go`:

```go
package config

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStore_HotReload(t *testing.T) {
	dir := t.TempDir()
	writeReg(t, dir, "a.yaml", "prefix: /a\nupstream: s1:9090\n")

	st, err := NewStore(dir)
	require.NoError(t, err)
	var reloadErrs atomic.Int32
	st.OnReloadError = func(error) { reloadErrs.Add(1) }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go st.Watch(ctx, slog.New(slog.NewTextHandler(os.Stderr, nil)))

	_, ok := st.Get().Match("/b/x/y")
	require.False(t, ok)

	// add a second registration -> picked up
	writeReg(t, dir, "b.yaml", "prefix: /b\nupstream: s2:9090\n")
	require.Eventually(t, func() bool {
		_, ok := st.Get().Match("/b/x/y")
		return ok
	}, 5*time.Second, 50*time.Millisecond)

	// break a file -> snapshot stays, error callback fires
	writeReg(t, dir, "b.yaml", "prefix: [broken\n")
	require.Eventually(t, func() bool { return reloadErrs.Load() >= 1 }, 5*time.Second, 50*time.Millisecond)
	_, ok = st.Get().Match("/b/x/y")
	require.True(t, ok, "last good snapshot must survive a bad reload")
}

func TestNewStore_FailsOnInvalidDir(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bad.yaml"), []byte("prefix: /events\nupstream: s:9090\n"), 0o644))
	_, err := NewStore(dir)
	require.Error(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestStore -v`
Expected: FAIL — `NewStore` undefined.

- [ ] **Step 3: Implement `internal/config/store.go`**

```bash
go get github.com/fsnotify/fsnotify
```

```go
package config

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Store holds the current Snapshot and hot-reloads it on directory changes.
// Kubelet updates ConfigMap volumes by swapping a ..data symlink, which
// surfaces as Create/Rename events on the watched directory.
type Store struct {
	dir string
	cur atomic.Pointer[Snapshot]

	// OnReloadError is called (if set) when a reload fails; the previous
	// snapshot stays live. Set before calling Watch.
	OnReloadError func(error)
}

func NewStore(dir string) (*Store, error) {
	snap, err := LoadDir(dir)
	if err != nil {
		return nil, err
	}
	st := &Store{dir: dir}
	st.cur.Store(snap)
	return st, nil
}

func (s *Store) Get() *Snapshot { return s.cur.Load() }

func (s *Store) Watch(ctx context.Context, logger *slog.Logger) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Error("config watcher init failed", "err", err)
		return
	}
	defer w.Close()
	if err := w.Add(s.dir); err != nil {
		logger.Error("config watch failed", "dir", s.dir, "err", err)
		return
	}
	var timer *time.Timer
	reload := make(chan struct{}, 1)
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-w.Events:
			if !ok {
				return
			}
			// debounce bursts (editors and kubelet emit several events)
			if timer != nil {
				timer.Stop()
			}
			timer = time.AfterFunc(200*time.Millisecond, func() {
				select {
				case reload <- struct{}{}:
				default:
				}
			})
		case err, ok := <-w.Errors:
			if !ok {
				return
			}
			logger.Error("config watch error", "err", err)
		case <-reload:
			snap, err := LoadDir(s.dir)
			if err != nil {
				logger.Error("config reload failed; keeping last good config", "err", err)
				if s.OnReloadError != nil {
					s.OnReloadError(err)
				}
				continue
			}
			s.cur.Store(snap)
			logger.Info("config reloaded", "registrations", len(snap.regs))
		}
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/config/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum internal/config/store.go internal/config/store_test.go
git commit -m "Add config store with fsnotify hot reload"
```

---

### Task 4: Zitadel JWT verifier + rule authorization

**Files:**
- Create: `internal/auth/verifier.go`
- Create: `internal/auth/rules.go`
- Create: `internal/auth/authtest/authtest.go`
- Test: `internal/auth/auth_test.go`

**Interfaces:**
- Consumes: `config.ValidRule` semantics (rule strings validated at load time).
- Produces (package `auth`):
  - `type Identity struct { Sub string; Roles map[string]struct{}; Authenticated bool }`
  - `func NewVerifier(ctx context.Context, issuer, jwksURL string, logger *slog.Logger) *Verifier` — background JWKS init with retry; never blocks
  - `func (v *Verifier) Ready() bool`
  - `func (v *Verifier) Verify(token string) (Identity, error)` — `ErrNotReady` or invalid-token error
  - `type AuthError struct { Status int; Code, Message string }` implementing `error`
  - `func Authorize(v *Verifier, rule, authorization string) (Identity, *AuthError)` — nil AuthError on success
- Produces (package `authtest`): `func NewJWKS(t *testing.T) *JWKS` with `(*JWKS).Server() *httptest.Server`, `(*JWKS).Sign(t, claims jwt.MapClaims) string`

- [ ] **Step 1: Add dependencies**

```bash
go get github.com/golang-jwt/jwt/v5 github.com/MicahParks/keyfunc/v3
```

- [ ] **Step 2: Write the test helper `internal/auth/authtest/authtest.go`**

```go
// Package authtest provides a fake JWKS endpoint and token signer for tests.
package authtest

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

const KID = "test-key-1"

type JWKS struct {
	Key *rsa.PrivateKey
}

func NewJWKS(t *testing.T) *JWKS {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return &JWKS{Key: key}
}

// Server serves the public key as a JWK Set; closed via t.Cleanup by callers.
func (j *JWKS) Server(t *testing.T) *httptest.Server {
	t.Helper()
	pub := &j.Key.PublicKey
	n := base64.RawURLEncoding.EncodeToString(pub.N.Bytes())
	e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes())
	body := fmt.Sprintf(`{"keys":[{"kty":"RSA","use":"sig","alg":"RS256","kid":%q,"n":%q,"e":%q}]}`, KID, n, e)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// Sign issues an RS256 token with the given claims and the test kid.
func (j *JWKS) Sign(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = KID
	s, err := tok.SignedString(j.Key)
	require.NoError(t, err)
	return s
}
```

- [ ] **Step 3: Write the failing test `internal/auth/auth_test.go`**

```go
package auth

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"

	"github.com/the-algovn/api-control-plane/internal/auth/authtest"
)

const issuer = "https://id.algovn.com"

func newVerifier(t *testing.T) (*Verifier, *authtest.JWKS) {
	t.Helper()
	jwks := authtest.NewJWKS(t)
	srv := jwks.Server(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	v := NewVerifier(ctx, issuer, srv.URL, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	require.Eventually(t, v.Ready, 5*time.Second, 20*time.Millisecond)
	return v, jwks
}

func validClaims() jwt.MapClaims {
	return jwt.MapClaims{
		"iss": issuer,
		"sub": "user-123",
		"exp": time.Now().Add(time.Hour).Unix(),
		"urn:zitadel:iam:org:project:roles": map[string]any{
			"admin": map[string]any{"318": "algovn.com"},
		},
	}
}

func TestVerify(t *testing.T) {
	v, jwks := newVerifier(t)

	id, err := v.Verify(jwks.Sign(t, validClaims()))
	require.NoError(t, err)
	require.Equal(t, "user-123", id.Sub)
	require.True(t, id.Authenticated)
	require.Contains(t, id.Roles, "admin")

	// wrong issuer
	c := validClaims()
	c["iss"] = "https://evil.example"
	_, err = v.Verify(jwks.Sign(t, c))
	require.Error(t, err)

	// expired
	c = validClaims()
	c["exp"] = time.Now().Add(-2 * time.Hour).Unix()
	_, err = v.Verify(jwks.Sign(t, c))
	require.Error(t, err)

	// garbage
	_, err = v.Verify("not.a.jwt")
	require.Error(t, err)
}

func TestAuthorize(t *testing.T) {
	v, jwks := newVerifier(t)
	valid := "Bearer " + jwks.Sign(t, validClaims())
	noRoles := validClaims()
	delete(noRoles, "urn:zitadel:iam:org:project:roles")
	member := "Bearer " + jwks.Sign(t, noRoles)

	cases := []struct {
		name, rule, authz string
		wantStatus        int // 0 = allowed
		wantSub           string
	}{
		{"anon no token", "anonymous", "", 0, ""},
		{"anon valid token", "anonymous", valid, 0, "user-123"},
		{"anon bad token", "anonymous", "Bearer garbage", 401, ""},
		{"authed no token", "authenticated", "", 401, ""},
		{"authed valid", "authenticated", valid, 0, "user-123"},
		{"role match", "role:admin", valid, 0, "user-123"},
		{"role missing", "role:admin", member, 403, ""},
		{"role no token", "role:admin", "", 401, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, aerr := Authorize(v, tc.rule, tc.authz)
			if tc.wantStatus == 0 {
				require.Nil(t, aerr)
				require.Equal(t, tc.wantSub, id.Sub)
			} else {
				require.NotNil(t, aerr)
				require.Equal(t, tc.wantStatus, aerr.Status)
			}
		})
	}
}

func TestAuthorize_NotReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// point at a dead JWKS endpoint: verifier never becomes ready
	v := NewVerifier(ctx, issuer, "http://127.0.0.1:1/jwks", slog.New(slog.NewTextHandler(os.Stderr, nil)))

	_, aerr := Authorize(v, "authenticated", "Bearer whatever")
	require.NotNil(t, aerr)
	require.Equal(t, 503, aerr.Status)

	// anonymous without a token still works while JWKS is down
	id, aerr := Authorize(v, "anonymous", "")
	require.Nil(t, aerr)
	require.False(t, id.Authenticated)
}
```

- [ ] **Step 4: Run test to verify it fails**

Run: `go test ./internal/auth/ -v`
Expected: FAIL — `NewVerifier` undefined.

- [ ] **Step 5: Implement `internal/auth/verifier.go`**

```go
// Package auth verifies Zitadel-issued Bearer JWTs and evaluates route rules.
package auth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

const rolesClaim = "urn:zitadel:iam:org:project:roles"

var ErrNotReady = errors.New("jwks not loaded yet")

type Identity struct {
	Sub           string
	Roles         map[string]struct{}
	Authenticated bool
}

type Verifier struct {
	issuer string
	kf     atomic.Pointer[jwt.Keyfunc]
}

// NewVerifier starts a background loop that fetches the JWKS with backoff.
// keyfunc.NewDefaultCtx auto-refreshes (hourly + rate-limited on unknown kid),
// so Zitadel key rotation needs no restarts.
func NewVerifier(ctx context.Context, issuer, jwksURL string, logger *slog.Logger) *Verifier {
	v := &Verifier{issuer: issuer}
	go func() {
		backoff := time.Second
		for {
			kf, err := keyfunc.NewDefaultCtx(ctx, []string{jwksURL})
			if err == nil {
				f := kf.Keyfunc
				v.kf.Store(&f)
				logger.Info("jwks loaded", "url", jwksURL)
				return
			}
			logger.Warn("jwks fetch failed; retrying", "url", jwksURL, "backoff", backoff, "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
		}
	}()
	return v
}

func (v *Verifier) Ready() bool { return v.kf.Load() != nil }

func (v *Verifier) Verify(token string) (Identity, error) {
	kf := v.kf.Load()
	if kf == nil {
		return Identity{}, ErrNotReady
	}
	claims := jwt.MapClaims{}
	_, err := jwt.ParseWithClaims(token, claims, *kf,
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithIssuer(v.issuer),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(30*time.Second),
	)
	if err != nil {
		return Identity{}, fmt.Errorf("invalid token: %w", err)
	}
	sub, _ := claims.GetSubject()
	id := Identity{Sub: sub, Roles: map[string]struct{}{}, Authenticated: true}
	if raw, ok := claims[rolesClaim].(map[string]any); ok {
		for role := range raw {
			id.Roles[role] = struct{}{}
		}
	}
	return id, nil
}
```

- [ ] **Step 6: Implement `internal/auth/rules.go`**

```go
package auth

import (
	"errors"
	"strings"
)

type AuthError struct {
	Status  int
	Code    string
	Message string
}

func (e *AuthError) Error() string { return e.Message }

var (
	errUnauthorized = &AuthError{Status: 401, Code: "unauthenticated", Message: "missing or invalid bearer token"}
	errForbidden    = &AuthError{Status: 403, Code: "permission_denied", Message: "insufficient role"}
	errNotReady     = &AuthError{Status: 503, Code: "unavailable", Message: "auth keys not loaded yet"}
)

// Authorize enforces a route/channel rule against the raw Authorization
// header. Rules are validated at config-load time (config.ValidRule); an
// unknown rule here fails closed as 403.
func Authorize(v *Verifier, rule, authorization string) (Identity, *AuthError) {
	token, hasToken := strings.CutPrefix(authorization, "Bearer ")
	if authorization != "" && !hasToken {
		return Identity{}, errUnauthorized // non-Bearer Authorization header
	}

	if rule == "anonymous" && !hasToken {
		return Identity{}, nil
	}
	if !hasToken {
		return Identity{}, errUnauthorized
	}

	id, err := v.Verify(token)
	switch {
	case errors.Is(err, ErrNotReady):
		return Identity{}, errNotReady
	case err != nil:
		return Identity{}, errUnauthorized
	}

	switch {
	case rule == "anonymous", rule == "authenticated":
		return id, nil
	case strings.HasPrefix(rule, "role:"):
		if _, ok := id.Roles[strings.TrimPrefix(rule, "role:")]; ok {
			return id, nil
		}
		return Identity{}, errForbidden
	default:
		return Identity{}, errForbidden
	}
}
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `go test ./internal/auth/... -v`
Expected: PASS (TestVerify, TestAuthorize all subtests, TestAuthorize_NotReady).

- [ ] **Step 8: Commit**

```bash
git add go.mod go.sum internal/auth/
git commit -m "Add Zitadel JWKS verifier and route rule authorization"
```

---

### Task 5: Test gRPC fixture + reflection descriptor registry

**Files:**
- Create: `internal/testsvc/buf.yaml`, `internal/testsvc/buf.gen.yaml`, `internal/testsvc/test.proto`
- Create: `internal/testsvc/gen/…` (generated, committed)
- Create: `internal/testsvc/server.go`
- Create: `internal/transcode/registry.go`
- Test: `internal/transcode/registry_test.go`

**Interfaces:**
- Consumes: `config.Registration` (Task 2).
- Produces (package `testsvc`): `func StartServer(t *testing.T) string` — in-process gRPC server (TCP on 127.0.0.1:0) serving `algovn.testsvc.v1.TestService` (`Echo`, `Fail`, `Slow`) with reflection; auto-stopped via `t.Cleanup`.
- Produces (package `transcode`):
  - `var ErrBackendNotReady = errors.New(…)`, `var ErrMethodNotFound = errors.New(…)`
  - `func NewRegistry(logger *slog.Logger) *Registry`
  - `func (r *Registry) Reconcile(ctx context.Context, regs []*config.Registration)` — idempotent single pass: dial+reflect missing upstreams (5s timeout each, failure logged and left absent for the next pass), close removed ones
  - `func (r *Registry) Backend(prefix string) (*Backend, error)` — `ErrBackendNotReady` if absent
  - `func (b *Backend) Method(svcMethod string) (protoreflect.MethodDescriptor, error)` — key `"pkg.Service/Method"`, `ErrMethodNotFound`
  - `func (r *Registry) Close()`

- [ ] **Step 1: Write the fixture proto and generate**

`internal/testsvc/test.proto`:

```proto
syntax = "proto3";

package algovn.testsvc.v1;

option go_package = "github.com/the-algovn/api-control-plane/internal/testsvc/gen;testsvcv1";

service TestService {
  rpc Echo(EchoRequest) returns (EchoResponse);
  rpc Fail(FailRequest) returns (EchoResponse);
  rpc Slow(SlowRequest) returns (EchoResponse);
}

message EchoRequest {
  string message = 1;
  int32 number = 2;
}

message EchoResponse {
  string message = 1;
  int32 number = 2;
}

message FailRequest {
  int32 code = 1;
  string message = 2;
}

message SlowRequest {
  int64 delay_ms = 1;
}
```

`internal/testsvc/buf.yaml`:

```yaml
version: v2
modules:
  - path: .
lint:
  use: [STANDARD]
```

`internal/testsvc/buf.gen.yaml`:

```yaml
version: v2
plugins:
  - remote: buf.build/protocolbuffers/go
    out: gen
    opt: paths=source_relative
  - remote: buf.build/grpc/go
    out: gen
    opt: paths=source_relative
```

```bash
cd /Users/duclm27/the-algovn/api-control-plane/internal/testsvc
buf generate
ls gen/
cd /Users/duclm27/the-algovn/api-control-plane
go get google.golang.org/grpc google.golang.org/protobuf
```

Expected: `gen/` contains `test.pb.go` and `test_grpc.pb.go`.

- [ ] **Step 2: Write `internal/testsvc/server.go`**

```go
// Package testsvc is a test-only gRPC upstream with server reflection.
package testsvc

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	testsvcv1 "github.com/the-algovn/api-control-plane/internal/testsvc/gen"
)

type server struct {
	testsvcv1.UnimplementedTestServiceServer
}

func (server) Echo(_ context.Context, req *testsvcv1.EchoRequest) (*testsvcv1.EchoResponse, error) {
	return &testsvcv1.EchoResponse{Message: req.GetMessage(), Number: req.GetNumber()}, nil
}

func (server) Fail(_ context.Context, req *testsvcv1.FailRequest) (*testsvcv1.EchoResponse, error) {
	return nil, status.Error(codes.Code(req.GetCode()), req.GetMessage())
}

func (server) Slow(ctx context.Context, req *testsvcv1.SlowRequest) (*testsvcv1.EchoResponse, error) {
	select {
	case <-time.After(time.Duration(req.GetDelayMs()) * time.Millisecond):
		return &testsvcv1.EchoResponse{Message: "done"}, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

// StartServer runs the fixture on a random localhost port and returns its address.
func StartServer(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	testsvcv1.RegisterTestServiceServer(s, server{})
	reflection.Register(s)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}
```

- [ ] **Step 3: Write the failing registry test**

`internal/transcode/registry_test.go`:

```go
package transcode

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/the-algovn/api-control-plane/internal/config"
	"github.com/the-algovn/api-control-plane/internal/testsvc"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(os.Stderr, nil)) }

func reg(prefix, upstream string) *config.Registration {
	return &config.Registration{Prefix: prefix, Upstream: upstream}
}

func TestRegistry_ReconcileAndResolve(t *testing.T) {
	addr := testsvc.StartServer(t)
	r := NewRegistry(testLogger())
	defer r.Close()

	r.Reconcile(context.Background(), []*config.Registration{reg("/test", addr)})

	b, err := r.Backend("/test")
	require.NoError(t, err)

	md, err := b.Method("algovn.testsvc.v1.TestService/Echo")
	require.NoError(t, err)
	require.Equal(t, "Echo", string(md.Name()))

	_, err = b.Method("algovn.testsvc.v1.TestService/Nope")
	require.ErrorIs(t, err, ErrMethodNotFound)

	// unknown prefix
	_, err = r.Backend("/ghost")
	require.ErrorIs(t, err, ErrBackendNotReady)
}

func TestRegistry_UnreachableUpstreamThenRecovers(t *testing.T) {
	r := NewRegistry(testLogger())
	defer r.Close()

	// nothing listening on this port
	r.Reconcile(context.Background(), []*config.Registration{reg("/test", "127.0.0.1:1")})
	_, err := r.Backend("/test")
	require.ErrorIs(t, err, ErrBackendNotReady)

	// upstream appears; next reconcile pass picks it up
	addr := testsvc.StartServer(t)
	r.Reconcile(context.Background(), []*config.Registration{reg("/test", addr)})
	b, err := r.Backend("/test")
	require.NoError(t, err)
	_, err = b.Method("algovn.testsvc.v1.TestService/Echo")
	require.NoError(t, err)
}

func TestRegistry_RemovesDroppedPrefixes(t *testing.T) {
	addr := testsvc.StartServer(t)
	r := NewRegistry(testLogger())
	defer r.Close()

	r.Reconcile(context.Background(), []*config.Registration{reg("/test", addr)})
	_, err := r.Backend("/test")
	require.NoError(t, err)

	r.Reconcile(context.Background(), nil)
	_, err = r.Backend("/test")
	require.ErrorIs(t, err, ErrBackendNotReady)
}
```

- [ ] **Step 4: Run test to verify it fails**

Run: `go test ./internal/transcode/ -v`
Expected: FAIL — `NewRegistry` undefined.

- [ ] **Step 5: Implement `internal/transcode/registry.go`**

```bash
go get github.com/jhump/protoreflect
```

```go
// Package transcode resolves gRPC method descriptors via server reflection
// and invokes unary methods with JSON bodies.
package transcode

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jhump/protoreflect/grpcreflect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/the-algovn/api-control-plane/internal/config"
)

var (
	ErrBackendNotReady = errors.New("upstream descriptors not loaded")
	ErrMethodNotFound  = errors.New("method not found")
)

type Backend struct {
	conn    *grpc.ClientConn
	methods map[string]protoreflect.MethodDescriptor // "pkg.Service/Method"
}

func (b *Backend) Method(svcMethod string) (protoreflect.MethodDescriptor, error) {
	md, ok := b.methods[svcMethod]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMethodNotFound, svcMethod)
	}
	return md, nil
}

type Registry struct {
	logger *slog.Logger

	mu       sync.RWMutex
	byPrefix map[string]*Backend // ready backends only
}

func NewRegistry(logger *slog.Logger) *Registry {
	return &Registry{logger: logger, byPrefix: map[string]*Backend{}}
}

func (r *Registry) Backend(prefix string) (*Backend, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	b, ok := r.byPrefix[prefix]
	if !ok {
		return nil, ErrBackendNotReady
	}
	return b, nil
}

// Reconcile makes the ready-backend set match regs: dials and reflects
// missing upstreams (failures are logged and retried on the next call),
// closes backends whose prefix disappeared. Call at startup, on config
// reload, and on a ticker to pick up late-starting upstreams.
func (r *Registry) Reconcile(ctx context.Context, regs []*config.Registration) {
	desired := map[string]*config.Registration{}
	for _, reg := range regs {
		desired[reg.Prefix] = reg
	}

	r.mu.Lock()
	for prefix, b := range r.byPrefix {
		if _, keep := desired[prefix]; !keep {
			_ = b.conn.Close()
			delete(r.byPrefix, prefix)
			r.logger.Info("backend removed", "prefix", prefix)
		}
	}
	missing := map[string]*config.Registration{}
	for prefix, reg := range desired {
		if _, ok := r.byPrefix[prefix]; !ok {
			missing[prefix] = reg
		}
	}
	r.mu.Unlock()

	for prefix, reg := range missing {
		b, err := r.connect(ctx, reg.Upstream)
		if err != nil {
			r.logger.Warn("backend not ready; will retry on next reconcile",
				"prefix", prefix, "upstream", reg.Upstream, "err", err)
			continue
		}
		r.mu.Lock()
		r.byPrefix[prefix] = b
		r.mu.Unlock()
		r.logger.Info("backend ready", "prefix", prefix, "methods", len(b.methods))
	}
}

func (r *Registry) connect(ctx context.Context, upstream string) (*Backend, error) {
	conn, err := grpc.NewClient(upstream,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`),
	)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rc := grpcreflect.NewClientAuto(rctx, conn)
	defer rc.Reset()

	svcs, err := rc.ListServices()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("reflection ListServices: %w", err)
	}
	methods := map[string]protoreflect.MethodDescriptor{}
	for _, s := range svcs {
		if strings.HasPrefix(s, "grpc.") {
			continue // reflection/health plumbing
		}
		sd, err := rc.ResolveService(s)
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("resolve %s: %w", s, err)
		}
		for _, m := range sd.GetMethods() {
			if m.IsClientStreaming() || m.IsServerStreaming() {
				continue // v1 transcodes unary only
			}
			methods[s+"/"+m.GetName()] = m.UnwrapMethod()
		}
	}
	if len(methods) == 0 {
		_ = conn.Close()
		return nil, errors.New("no unary methods exposed via reflection")
	}
	return &Backend{conn: conn, methods: methods}, nil
}

func (r *Registry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for prefix, b := range r.byPrefix {
		_ = b.conn.Close()
		delete(r.byPrefix, prefix)
	}
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/transcode/ -v`
Expected: PASS (3 tests).

- [ ] **Step 7: Commit**

```bash
git add go.mod go.sum internal/testsvc/ internal/transcode/registry.go internal/transcode/registry_test.go
git commit -m "Add test gRPC fixture and reflection-based descriptor registry"
```

---

### Task 6: JSON ⇄ gRPC transcoder + error mapping

**Files:**
- Create: `internal/transcode/invoke.go`
- Test: `internal/transcode/invoke_test.go`

**Interfaces:**
- Consumes: `Backend` (Task 5), `testsvc.StartServer`.
- Produces:
  - `func (b *Backend) Invoke(ctx context.Context, svcMethod string, jsonBody []byte, md metadata.MD, deadline time.Duration) ([]byte, error)` — returns response JSON; errors are gRPC `status` errors (or wrapped `ErrMethodNotFound`); malformed request JSON → `InvalidArgument` status
  - `func HTTPStatus(c codes.Code) int` — spec §6 mapping

- [ ] **Step 1: Write the failing test**

`internal/transcode/invoke_test.go`:

```go
package transcode

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/the-algovn/api-control-plane/internal/config"
	"github.com/the-algovn/api-control-plane/internal/testsvc"
)

func readyBackend(t *testing.T) *Backend {
	t.Helper()
	addr := testsvc.StartServer(t)
	r := NewRegistry(testLogger())
	t.Cleanup(r.Close)
	r.Reconcile(context.Background(), []*config.Registration{reg("/test", addr)})
	b, err := r.Backend("/test")
	require.NoError(t, err)
	return b
}

func TestInvoke_Echo(t *testing.T) {
	b := readyBackend(t)
	out, err := b.Invoke(context.Background(), "algovn.testsvc.v1.TestService/Echo",
		[]byte(`{"message":"hi","number":7}`), metadata.MD{}, time.Second)
	require.NoError(t, err)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(out, &resp))
	require.Equal(t, "hi", resp["message"])
	require.EqualValues(t, 7, resp["number"])
}

func TestInvoke_EmptyBody(t *testing.T) {
	b := readyBackend(t)
	out, err := b.Invoke(context.Background(), "algovn.testsvc.v1.TestService/Echo",
		nil, metadata.MD{}, time.Second)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(out))
}

func TestInvoke_BadJSON(t *testing.T) {
	b := readyBackend(t)
	_, err := b.Invoke(context.Background(), "algovn.testsvc.v1.TestService/Echo",
		[]byte(`{"nope":true}`), metadata.MD{}, time.Second)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
}

func TestInvoke_UpstreamError(t *testing.T) {
	b := readyBackend(t)
	_, err := b.Invoke(context.Background(), "algovn.testsvc.v1.TestService/Fail",
		[]byte(`{"code":5,"message":"gone"}`), metadata.MD{}, time.Second)
	st, _ := status.FromError(err)
	require.Equal(t, codes.NotFound, st.Code())
	require.Equal(t, "gone", st.Message())
}

func TestInvoke_Deadline(t *testing.T) {
	b := readyBackend(t)
	_, err := b.Invoke(context.Background(), "algovn.testsvc.v1.TestService/Slow",
		[]byte(`{"delayMs":"2000"}`), metadata.MD{}, 100*time.Millisecond)
	st, _ := status.FromError(err)
	require.Equal(t, codes.DeadlineExceeded, st.Code())
}

func TestInvoke_UnknownMethod(t *testing.T) {
	b := readyBackend(t)
	_, err := b.Invoke(context.Background(), "algovn.testsvc.v1.TestService/Nope",
		nil, metadata.MD{}, time.Second)
	require.ErrorIs(t, err, ErrMethodNotFound)
}

func TestHTTPStatus(t *testing.T) {
	cases := map[codes.Code]int{
		codes.NotFound:         404,
		codes.PermissionDenied: 403,
		codes.InvalidArgument:  400,
		codes.Unavailable:      502,
		codes.DeadlineExceeded: 504,
		codes.Unauthenticated:  401,
		codes.Internal:         500,
		codes.Unknown:          500,
	}
	for code, want := range cases {
		require.Equal(t, want, HTTPStatus(code), code.String())
	}
}
```

> Note: `delayMs` is int64 → protojson accepts string-encoded 64-bit ints (`"2000"`); this is deliberate protojson behavior worth pinning in a test.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/transcode/ -run TestInvoke -v`
Expected: FAIL — `Invoke` undefined.

- [ ] **Step 3: Implement `internal/transcode/invoke.go`**

```go
package transcode

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/dynamicpb"
)

var marshaler = protojson.MarshalOptions{} // proto3 JSON defaults

// Invoke calls a unary method with a JSON request body and returns the JSON
// response. Errors are gRPC status errors (map with HTTPStatus), except
// ErrMethodNotFound which the caller maps to 404.
func (b *Backend) Invoke(ctx context.Context, svcMethod string, jsonBody []byte, md metadata.MD, deadline time.Duration) ([]byte, error) {
	desc, err := b.Method(svcMethod)
	if err != nil {
		return nil, err
	}
	in := dynamicpb.NewMessage(desc.Input())
	if len(jsonBody) > 0 {
		if err := protojson.Unmarshal(jsonBody, in); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "request body: %v", err)
		}
	}
	out := dynamicpb.NewMessage(desc.Output())

	ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(ctx, md), deadline)
	defer cancel()
	if err := b.conn.Invoke(ctx, "/"+svcMethod, in, out); err != nil {
		return nil, err // already a status error
	}
	respJSON, err := marshaler.Marshal(out)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal response: %v", err)
	}
	return respJSON, nil
}

// HTTPStatus maps gRPC codes to HTTP statuses (spec §6).
func HTTPStatus(c codes.Code) int {
	switch c {
	case codes.OK:
		return 200
	case codes.InvalidArgument:
		return 400
	case codes.Unauthenticated:
		return 401
	case codes.PermissionDenied:
		return 403
	case codes.NotFound:
		return 404
	case codes.Unavailable:
		return 502
	case codes.DeadlineExceeded:
		return 504
	default:
		return 500
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/transcode/ -v`
Expected: PASS (all).

- [ ] **Step 5: Commit**

```bash
git add internal/transcode/invoke.go internal/transcode/invoke_test.go
git commit -m "Add JSON-to-gRPC unary transcoding and HTTP status mapping"
```

---

### Task 7: SSE hub

**Files:**
- Create: `internal/push/hub.go`
- Test: `internal/push/hub_test.go`

**Interfaces:**
- Produces (package `push`):
  - `func NewHub() *Hub`
  - `func (h *Hub) SetBindingHooks(onFirst, onLast func(channel string))` — called (outside locks) when a channel gains its first / loses its last subscriber
  - `func (h *Hub) Subscribe(channel string) *Subscription` — `Subscription.C <-chan []byte` (buffer 16), `Subscription.Close()`
  - `func (h *Hub) Publish(channel string, data []byte)` — non-blocking; a subscriber with a full buffer is closed (slow-client eviction)
  - `func (h *Hub) CloseAll()` — closes every subscription (used on broker disconnect)
  - `func (h *Hub) ActiveChannels() []string`
  - `func (h *Hub) Counts() map[string]int` (for the SSE metrics gauge)

- [ ] **Step 1: Write the failing test**

`internal/push/hub_test.go`:

```go
package push

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func recvOne(t *testing.T, c <-chan []byte) []byte {
	t.Helper()
	select {
	case b, ok := <-c:
		require.True(t, ok, "subscription closed unexpectedly")
		return b
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
		return nil
	}
}

func TestHub_PublishFanOut(t *testing.T) {
	h := NewHub()
	s1 := h.Subscribe("demo.ping")
	s2 := h.Subscribe("demo.ping")
	other := h.Subscribe("other.chan")
	defer s1.Close()
	defer s2.Close()
	defer other.Close()

	h.Publish("demo.ping", []byte(`{"n":1}`))
	require.JSONEq(t, `{"n":1}`, string(recvOne(t, s1.C)))
	require.JSONEq(t, `{"n":1}`, string(recvOne(t, s2.C)))
	select {
	case <-other.C:
		t.Fatal("wrong channel received event")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestHub_BindingHooks(t *testing.T) {
	h := NewHub()
	var mu sync.Mutex
	var events []string
	h.SetBindingHooks(
		func(ch string) { mu.Lock(); events = append(events, "first:"+ch); mu.Unlock() },
		func(ch string) { mu.Lock(); events = append(events, "last:"+ch); mu.Unlock() },
	)
	s1 := h.Subscribe("a.b")
	s2 := h.Subscribe("a.b")
	s1.Close()
	s2.Close()
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"first:a.b", "last:a.b"}, events)
}

func TestHub_SlowClientEvicted(t *testing.T) {
	h := NewHub()
	s := h.Subscribe("a.b")
	for range 20 { // buffer is 16; overflow forces eviction
		h.Publish("a.b", []byte(`{}`))
	}
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-s.C:
			if !ok {
				return // closed: evicted as expected
			}
		case <-deadline:
			t.Fatal("slow client was not evicted")
		}
	}
}

func TestHub_CloseAllAndActiveChannels(t *testing.T) {
	h := NewHub()
	s := h.Subscribe("a.b")
	require.Equal(t, []string{"a.b"}, h.ActiveChannels())
	require.Equal(t, map[string]int{"a.b": 1}, h.Counts())
	h.CloseAll()
	_, ok := <-s.C
	require.False(t, ok)
	require.Empty(t, h.ActiveChannels())

	// double Close is safe
	s.Close()
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/push/ -v`
Expected: FAIL — `NewHub` undefined.

- [ ] **Step 3: Implement `internal/push/hub.go`**

```go
// Package push fans RabbitMQ events out to SSE subscribers.
package push

import (
	"sort"
	"sync"
)

const subscriberBuffer = 16

type Subscription struct {
	C       <-chan []byte
	ch      chan []byte
	channel string
	hub     *Hub
	once    sync.Once
}

func (s *Subscription) Close() { s.hub.unsubscribe(s) }

type Hub struct {
	mu      sync.Mutex
	subs    map[string]map[*Subscription]struct{}
	onFirst func(string)
	onLast  func(string)
}

func NewHub() *Hub {
	return &Hub{subs: map[string]map[*Subscription]struct{}{}}
}

func (h *Hub) SetBindingHooks(onFirst, onLast func(channel string)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onFirst, h.onLast = onFirst, onLast
}

func (h *Hub) Subscribe(channel string) *Subscription {
	s := &Subscription{ch: make(chan []byte, subscriberBuffer), channel: channel, hub: h}
	s.C = s.ch

	h.mu.Lock()
	set, ok := h.subs[channel]
	if !ok {
		set = map[*Subscription]struct{}{}
		h.subs[channel] = set
	}
	set[s] = struct{}{}
	first := len(set) == 1
	onFirst := h.onFirst
	h.mu.Unlock()

	if first && onFirst != nil {
		onFirst(channel)
	}
	return s
}

func (h *Hub) unsubscribe(s *Subscription) {
	h.mu.Lock()
	set, ok := h.subs[s.channel]
	var last bool
	var onLast func(string)
	if ok {
		if _, present := set[s]; present {
			delete(set, s)
			if len(set) == 0 {
				delete(h.subs, s.channel)
				last, onLast = true, h.onLast
			}
		}
	}
	h.mu.Unlock()

	s.once.Do(func() { close(s.ch) })
	if last && onLast != nil {
		onLast(s.channel)
	}
}

// Publish delivers to every subscriber without blocking; a subscriber whose
// buffer is full is evicted (spec: slow clients are disconnected).
func (h *Hub) Publish(channel string, data []byte) {
	h.mu.Lock()
	var evict []*Subscription
	for s := range h.subs[channel] {
		select {
		case s.ch <- data:
		default:
			evict = append(evict, s)
		}
	}
	h.mu.Unlock()
	for _, s := range evict {
		h.unsubscribe(s)
	}
}

func (h *Hub) CloseAll() {
	h.mu.Lock()
	var all []*Subscription
	for _, set := range h.subs {
		for s := range set {
			all = append(all, s)
		}
	}
	h.mu.Unlock()
	for _, s := range all {
		s.hub.unsubscribe(s)
	}
}

func (h *Hub) ActiveChannels() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, 0, len(h.subs))
	for ch := range h.subs {
		out = append(out, ch)
	}
	sort.Strings(out)
	return out
}

func (h *Hub) Counts() map[string]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]int, len(h.subs))
	for ch, set := range h.subs {
		out[ch] = len(set)
	}
	return out
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/push/ -race -v`
Expected: PASS (4 tests, race-clean).

- [ ] **Step 5: Commit**

```bash
git add internal/push/hub.go internal/push/hub_test.go
git commit -m "Add SSE fan-out hub with slow-client eviction"
```

---

### Task 8: RabbitMQ consumer

**Files:**
- Create: `internal/push/consumer.go`
- Test: `internal/push/consumer_integration_test.go` (build tag `integration`)

**Interfaces:**
- Consumes: `Hub` (Task 7).
- Produces:
  - `func NewConsumer(url string, hub *Hub, logger *slog.Logger) *Consumer` — wires itself as the hub's binding hooks
  - `func (c *Consumer) Run(ctx context.Context)` — blocking reconnect loop; declares topic exchange `events` (durable); consumes an exclusive server-named queue; binds/unbinds routing keys as channels gain/lose subscribers; re-binds `hub.ActiveChannels()` after reconnect; on disconnect sets Connected=false and calls `hub.CloseAll()`
  - `func (c *Consumer) Connected() bool`

- [ ] **Step 1: Add dependencies**

```bash
go get github.com/rabbitmq/amqp091-go
go get github.com/testcontainers/testcontainers-go github.com/testcontainers/testcontainers-go/modules/rabbitmq
```

- [ ] **Step 2: Write the failing integration test**

`internal/push/consumer_integration_test.go`:

```go
//go:build integration

package push

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcrabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

func startRabbit(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := tcrabbit.Run(ctx, "rabbitmq:4.1-management-alpine")
	testcontainers.CleanupContainer(t, c)
	require.NoError(t, err)
	url, err := c.AmqpURL(ctx)
	require.NoError(t, err)
	return url
}

func TestConsumer_EndToEnd(t *testing.T) {
	url := startRabbit(t)
	hub := NewHub()
	cons := NewConsumer(url, hub, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go cons.Run(ctx)
	require.Eventually(t, cons.Connected, 15*time.Second, 100*time.Millisecond)

	sub := hub.Subscribe("demo.ping") // triggers a bind command
	defer sub.Close()

	pub, err := amqp.Dial(url)
	require.NoError(t, err)
	defer pub.Close()
	pch, err := pub.Channel()
	require.NoError(t, err)

	// binding is asynchronous — publish until the event lands
	var got []byte
	require.Eventually(t, func() bool {
		err := pch.PublishWithContext(context.Background(), "events", "demo.ping", false, false,
			amqp.Publishing{ContentType: "application/json", Body: []byte(`{"total":42}`)})
		require.NoError(t, err)
		select {
		case got = <-sub.C:
			return true
		case <-time.After(300 * time.Millisecond):
			return false
		}
	}, 15*time.Second, 100*time.Millisecond)
	require.JSONEq(t, `{"total":42}`, string(got))

	// events on unbound routing keys don't arrive
	other := hub.Subscribe("other.topic")
	defer other.Close()
	require.NoError(t, pch.PublishWithContext(context.Background(), "events", "unbound.key", false, false,
		amqp.Publishing{Body: []byte(`{}`)}))
	select {
	case <-other.C:
		t.Fatal("received event for a key nobody bound")
	case <-time.After(500 * time.Millisecond):
	}
}
```

- [ ] **Step 3: Run test to verify it fails to compile**

```bash
go vet -tags integration ./internal/push/
```
Expected: FAIL — `NewConsumer` undefined.

- [ ] **Step 4: Implement `internal/push/consumer.go`**

```go
package push

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const exchange = "events"

type bindCmd struct {
	bind    bool
	channel string
}

// Consumer owns the AMQP connection. All channel operations happen inside
// Run's goroutine; Bind/Unbind requests arrive over cmds (amqp channels are
// not safe for concurrent use).
type Consumer struct {
	url       string
	hub       *Hub
	logger    *slog.Logger
	cmds      chan bindCmd
	connected atomic.Bool
}

func NewConsumer(url string, hub *Hub, logger *slog.Logger) *Consumer {
	c := &Consumer{url: url, hub: hub, logger: logger, cmds: make(chan bindCmd, 64)}
	hub.SetBindingHooks(
		func(ch string) { c.cmds <- bindCmd{bind: true, channel: ch} },
		func(ch string) { c.cmds <- bindCmd{bind: false, channel: ch} },
	)
	return c
}

func (c *Consumer) Connected() bool { return c.connected.Load() }

func (c *Consumer) Run(ctx context.Context) {
	backoff := time.Second
	for {
		if err := c.session(ctx); err != nil {
			c.logger.Warn("rabbitmq session ended", "err", err, "retry_in", backoff)
		}
		c.connected.Store(false)
		c.hub.CloseAll() // spec: broker down => close SSE clients; EventSource reconnects
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		} else {
			backoff = time.Second // periodic reset keeps retries lively
		}
	}
}

func (c *Consumer) session(ctx context.Context) error {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	q, err := ch.QueueDeclare("", false, true, true, false, nil) // server-named, auto-delete, exclusive
	if err != nil {
		return err
	}
	for _, name := range c.hub.ActiveChannels() { // re-bind after reconnect
		if err := ch.QueueBind(q.Name, name, exchange, false, nil); err != nil {
			return err
		}
	}
	msgs, err := ch.Consume(q.Name, "", true, true, false, false, nil)
	if err != nil {
		return err
	}
	c.connected.Store(true)
	c.logger.Info("rabbitmq connected", "queue", q.Name)

	for {
		select {
		case <-ctx.Done():
			return nil
		case cmd := <-c.cmds:
			var err error
			if cmd.bind {
				err = ch.QueueBind(q.Name, cmd.channel, exchange, false, nil)
			} else {
				err = ch.QueueUnbind(q.Name, cmd.channel, exchange, nil)
			}
			if err != nil {
				return err
			}
		case m, ok := <-msgs:
			if !ok {
				return amqp.ErrClosed
			}
			c.hub.Publish(m.RoutingKey, m.Body)
		}
	}
}
```

- [ ] **Step 5: Run unit tests still pass, then the integration test**

```bash
go test ./internal/push/ -race -v
# integration (podman must be running: `podman machine start`)
export DOCKER_HOST="unix://$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}')"
export TESTCONTAINERS_RYUK_DISABLED=true
go test -tags integration ./internal/push/ -run TestConsumer_EndToEnd -v -timeout 300s
```

Expected: unit PASS; integration PASS (first run pulls the rabbitmq image — allow a few minutes).

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/push/consumer.go internal/push/consumer_integration_test.go
git commit -m "Add RabbitMQ consumer with reconnect and dynamic bindings"
```

---

### Task 9: HTTP server assembly, metrics, main()

**Files:**
- Create: `internal/observability/metrics.go`
- Create: `internal/httpserver/server.go`
- Create: `internal/httpserver/middleware.go`
- Create: `cmd/api-control-plane/main.go`
- Test: `internal/httpserver/server_test.go`

**Interfaces:**
- Consumes: everything from Tasks 2–8 (`config.Store/Snapshot`, `auth.Verifier/Authorize/AuthError`, `transcode.Registry/Backend/Invoke/HTTPStatus/ErrMethodNotFound/ErrBackendNotReady`, `push.Hub`, `authtest`, `testsvc`).
- Produces:
  - `observability.Metrics` struct: `Requests *prometheus.CounterVec` (labels `route`,`code`), `Duration *prometheus.HistogramVec` (label `route`), `SSEClients *prometheus.GaugeVec` (label `channel`), `ReloadErrors prometheus.Counter`; `func New(reg prometheus.Registerer) *Metrics`
  - `httpserver.Server` struct with fields `Store *config.Store; Verifier *auth.Verifier; Backends *transcode.Registry; Hub *push.Hub; RabbitConnected func() bool; CORSOrigins []string; Logger *slog.Logger; Metrics *observability.Metrics`; method `Handler() http.Handler`
  - `cmd/api-control-plane`: env `LISTEN_ADDR` (`:8080`), `METRICS_ADDR` (`:9091`), `REGISTRATIONS_DIR` (required), `ISSUER` (`https://id.algovn.com`), `JWKS_URL` (default `$ISSUER/oauth/v2/keys`), `AMQP_URL` (optional — SSE 503s when unset), `CORS_ORIGINS` (`https://*.algovn.com`, comma-separated)

- [ ] **Step 1: Implement `internal/observability/metrics.go`** (no test cycle of its own — asserted via the e2e test's scrape)

```bash
go get github.com/prometheus/client_golang
```

```go
// Package observability defines the service's Prometheus metrics.
package observability

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	Requests     *prometheus.CounterVec
	Duration     *prometheus.HistogramVec
	SSEClients   *prometheus.GaugeVec
	ReloadErrors prometheus.Counter
}

func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "acp_requests_total", Help: "API requests by registered route and HTTP code.",
		}, []string{"route", "code"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "acp_request_duration_seconds", Help: "API request latency by registered route.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route"}),
		SSEClients: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "acp_sse_clients", Help: "Connected SSE clients per channel.",
		}, []string{"channel"}),
		ReloadErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "acp_config_reload_errors_total", Help: "Failed registration reloads.",
		}),
	}
	reg.MustRegister(m.Requests, m.Duration, m.SSEClients, m.ReloadErrors)
	return m
}
```

- [ ] **Step 2: Write the failing e2e test**

`internal/httpserver/server_test.go`:

```go
package httpserver

import (
	"bufio"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/the-algovn/api-control-plane/internal/auth"
	"github.com/the-algovn/api-control-plane/internal/auth/authtest"
	"github.com/the-algovn/api-control-plane/internal/config"
	"github.com/the-algovn/api-control-plane/internal/observability"
	"github.com/the-algovn/api-control-plane/internal/push"
	"github.com/the-algovn/api-control-plane/internal/testsvc"
	"github.com/the-algovn/api-control-plane/internal/transcode"
)

const issuer = "https://id.algovn.com"

type fixture struct {
	srv  *httptest.Server
	jwks *authtest.JWKS
	hub  *push.Hub
}

func newFixture(t *testing.T, rabbitConnected bool) *fixture {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	upstream := testsvc.StartServer(t)

	dir := t.TempDir()
	regYAML := `
prefix: /test
upstream: ` + upstream + `
defaultRule: authenticated
routes:
  - method: algovn.testsvc.v1.TestService/Echo
    rule: anonymous
  - method: algovn.testsvc.v1.TestService/Fail
    rule: authenticated
  - method: algovn.testsvc.v1.TestService/Slow
    rule: role:admin
channels:
  - name: test.events
    rule: anonymous
  - name: test.private
    rule: authenticated
`
	require.NoError(t, os.WriteFile(dir+"/test.yaml", []byte(regYAML), 0o644))
	store, err := config.NewStore(dir)
	require.NoError(t, err)

	jwks := authtest.NewJWKS(t)
	jwksSrv := jwks.Server(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	verifier := auth.NewVerifier(ctx, issuer, jwksSrv.URL, logger)
	require.Eventually(t, verifier.Ready, 5*time.Second, 20*time.Millisecond)

	backends := transcode.NewRegistry(logger)
	t.Cleanup(backends.Close)
	backends.Reconcile(ctx, store.Get().Registrations())

	hub := push.NewHub()
	s := &Server{
		Store: store, Verifier: verifier, Backends: backends, Hub: hub,
		RabbitConnected: func() bool { return rabbitConnected },
		CORSOrigins:     []string{"https://*.algovn.com"},
		Logger:          logger,
		Metrics:         observability.New(prometheus.NewRegistry()),
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return &fixture{srv: srv, jwks: jwks, hub: hub}
}

func (f *fixture) token(t *testing.T, roles ...string) string {
	claims := jwt.MapClaims{"iss": issuer, "sub": "user-1", "exp": time.Now().Add(time.Hour).Unix()}
	if len(roles) > 0 {
		rm := map[string]any{}
		for _, r := range roles {
			rm[r] = map[string]any{"1": "algovn.com"}
		}
		claims["urn:zitadel:iam:org:project:roles"] = rm
	}
	return "Bearer " + f.jwks.Sign(t, claims)
}

func do(t *testing.T, method, url, authz, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	require.NoError(t, err)
	if authz != "" {
		req.Header.Set("Authorization", authz)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestTranscodeRoutes(t *testing.T) {
	f := newFixture(t, true)
	base := f.srv.URL

	// anonymous route, no token
	resp := do(t, "POST", base+"/test/algovn.testsvc.v1.TestService/Echo", "", `{"message":"hi"}`)
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	// authenticated route: 401 then 200
	resp = do(t, "POST", base+"/test/algovn.testsvc.v1.TestService/Fail", "", `{"code":0}`)
	require.Equal(t, 401, resp.StatusCode)
	resp = do(t, "POST", base+"/test/algovn.testsvc.v1.TestService/Fail", f.token(t), `{"code":0,"message":"x"}`)
	require.Equal(t, 200, resp.StatusCode) // codes.OK => success

	// upstream error mapping: NotFound(5) -> 404 with error body
	resp = do(t, "POST", base+"/test/algovn.testsvc.v1.TestService/Fail", f.token(t), `{"code":5,"message":"gone"}`)
	require.Equal(t, 404, resp.StatusCode)

	// role route: 403 without role, 200 with
	resp = do(t, "POST", base+"/test/algovn.testsvc.v1.TestService/Slow", f.token(t), `{"delayMs":"1"}`)
	require.Equal(t, 403, resp.StatusCode)
	resp = do(t, "POST", base+"/test/algovn.testsvc.v1.TestService/Slow", f.token(t, "admin"), `{"delayMs":"1"}`)
	require.Equal(t, 200, resp.StatusCode)

	// unknown prefix -> 404; unknown method -> 404; GET on rpc -> 405
	resp = do(t, "POST", base+"/ghost/x.Y/Z", "", `{}`)
	require.Equal(t, 404, resp.StatusCode)
	resp = do(t, "POST", base+"/test/algovn.testsvc.v1.TestService/Nope", f.token(t), `{}`)
	require.Equal(t, 404, resp.StatusCode)
	resp = do(t, "GET", base+"/test/algovn.testsvc.v1.TestService/Echo", "", "")
	require.Equal(t, 405, resp.StatusCode)

	// malformed rpc path (no Service/Method) -> 404
	resp = do(t, "POST", base+"/test/whatever", "", `{}`)
	require.Equal(t, 404, resp.StatusCode)
}

func TestCORS(t *testing.T) {
	f := newFixture(t, true)
	req, _ := http.NewRequest("OPTIONS", f.srv.URL+"/test/algovn.testsvc.v1.TestService/Echo", nil)
	req.Header.Set("Origin", "https://button.algovn.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 204, resp.StatusCode)
	require.Equal(t, "https://button.algovn.com", resp.Header.Get("Access-Control-Allow-Origin"))

	req2, _ := http.NewRequest("OPTIONS", f.srv.URL+"/test/x/y", nil)
	req2.Header.Set("Origin", "https://evil.example")
	req2.Header.Set("Access-Control-Request-Method", "POST")
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	require.Empty(t, resp2.Header.Get("Access-Control-Allow-Origin"))
}

func TestHealthz(t *testing.T) {
	f := newFixture(t, true)
	resp := do(t, "GET", f.srv.URL+"/healthz", "", "")
	require.Equal(t, 200, resp.StatusCode)
}

func TestSSE(t *testing.T) {
	f := newFixture(t, true)

	// unknown channel -> 404; protected channel without token -> 401
	resp := do(t, "GET", f.srv.URL+"/events/nope.nope", "", "")
	require.Equal(t, 404, resp.StatusCode)
	resp = do(t, "GET", f.srv.URL+"/events/test.private", "", "")
	require.Equal(t, 401, resp.StatusCode)

	// live stream: subscribe, then publish through the hub
	req, _ := http.NewRequest("GET", f.srv.URL+"/events/test.events", nil)
	sseResp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer sseResp.Body.Close()
	require.Equal(t, 200, sseResp.StatusCode)
	require.Equal(t, "text/event-stream", sseResp.Header.Get("Content-Type"))

	require.Eventually(t, func() bool {
		return len(f.hub.ActiveChannels()) == 1
	}, 2*time.Second, 20*time.Millisecond)
	f.hub.Publish("test.events", []byte(`{"total":1}`))

	reader := bufio.NewReader(sseResp.Body)
	deadline := time.After(5 * time.Second)
	lines := make(chan string, 10)
	go func() {
		for {
			l, err := reader.ReadString('\n')
			if err != nil {
				close(lines)
				return
			}
			lines <- strings.TrimRight(l, "\n")
		}
	}()
	for {
		select {
		case l := <-lines:
			if l == `data: {"total":1}` {
				return // success
			}
		case <-deadline:
			t.Fatal("SSE event not received")
		}
	}
}

func TestSSE_RabbitDown(t *testing.T) {
	f := newFixture(t, false)
	resp := do(t, "GET", f.srv.URL+"/events/test.events", "", "")
	require.Equal(t, 503, resp.StatusCode)
}

func TestBodyLimit(t *testing.T) {
	f := newFixture(t, true)
	big := `{"message":"` + strings.Repeat("x", 1<<20) + `"}` // > 1 MiB
	resp := do(t, "POST", f.srv.URL+"/test/algovn.testsvc.v1.TestService/Echo", "", big)
	require.Equal(t, 413, resp.StatusCode)
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/httpserver/ -v`
Expected: FAIL — `Server` undefined.

- [ ] **Step 4: Implement `internal/httpserver/middleware.go`**

```go
package httpserver

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "message": message})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// recoverLog wraps the whole handler chain: panic -> 500, plus access log.
func recoverLog(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: 200}
		start := time.Now()
		defer func() {
			if p := recover(); p != nil {
				logger.Error("panic recovered", "path", r.URL.Path, "panic", fmt.Sprint(p))
				writeError(rec, 500, "internal", "internal error")
			}
			logger.Info("request",
				"method", r.Method, "path", r.URL.Path,
				"status", rec.status, "duration_ms", time.Since(start).Milliseconds())
		}()
		next.ServeHTTP(rec, r)
	})
}

// corsMiddleware handles the *.algovn.com allowlist and preflight.
func corsMiddleware(allowed []string, next http.Handler) http.Handler {
	match := func(origin string) bool {
		for _, pat := range allowed {
			if pat == origin {
				return true
			}
			if scheme, host, ok := strings.Cut(pat, "://"); ok && strings.HasPrefix(host, "*.") {
				if strings.HasPrefix(origin, scheme+"://") &&
					strings.HasSuffix(strings.TrimPrefix(origin, scheme+"://"), strings.TrimPrefix(host, "*")) {
					return true
				}
			}
		}
		return false
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && match(origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			w.Header().Set("Access-Control-Max-Age", "600")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
```

- [ ] **Step 5: Implement `internal/httpserver/server.go`**

```go
// Package httpserver assembles routing, auth, transcoding and SSE.
package httpserver

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/the-algovn/api-control-plane/internal/auth"
	"github.com/the-algovn/api-control-plane/internal/config"
	"github.com/the-algovn/api-control-plane/internal/observability"
	"github.com/the-algovn/api-control-plane/internal/push"
	"github.com/the-algovn/api-control-plane/internal/transcode"
)

const (
	maxRequestBody = 1 << 20 // 1 MiB
	sseHeartbeat   = 25 * time.Second
)

var forwardedHeaders = []string{"authorization", "x-request-id", "accept-language"}

type Server struct {
	Store           *config.Store
	Verifier        *auth.Verifier
	Backends        *transcode.Registry
	Hub             *push.Hub
	RabbitConnected func() bool
	CORSOrigins     []string
	Logger          *slog.Logger
	Metrics         *observability.Metrics
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /events/{channel}", s.handleSSE)
	mux.HandleFunc("/", s.handleAPI)
	return recoverLog(s.Logger, corsMiddleware(s.CORSOrigins, mux))
}

func (s *Server) handleAPI(w http.ResponseWriter, r *http.Request) {
	snap := s.Store.Get()
	reg, ok := snap.Match(r.URL.Path)
	if !ok {
		writeError(w, 404, "not_found", "unknown API prefix")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, 405, "method_not_allowed", "RPC calls are POST")
		return
	}
	// /<prefix>/<pkg.Service>/<Method> -> "pkg.Service/Method"
	svcMethod := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, reg.Prefix), "/")
	parts := strings.Split(svcMethod, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		writeError(w, 404, "not_found", "expected /"+strings.TrimPrefix(reg.Prefix, "/")+"/<pkg.Service>/<Method>")
		return
	}

	rule, deadline := reg.RouteRule(svcMethod)
	if _, aerr := auth.Authorize(s.Verifier, rule, r.Header.Get("Authorization")); aerr != nil {
		s.count(reg.Prefix+"/"+svcMethod, aerr.Status)
		writeError(w, aerr.Status, aerr.Code, aerr.Message)
		return
	}

	backend, err := s.Backends.Backend(reg.Prefix)
	if err != nil {
		s.count(reg.Prefix+"/"+svcMethod, 502)
		writeError(w, 502, "unavailable", "upstream descriptors not loaded")
		return
	}

	body, rerr := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if rerr != nil {
		var mbe *http.MaxBytesError
		if errors.As(rerr, &mbe) {
			writeError(w, 413, "invalid_argument", "request body exceeds 1MiB")
			return
		}
		writeError(w, 400, "invalid_argument", "reading request body: "+rerr.Error())
		return
	}

	md := metadata.MD{}
	for _, h := range forwardedHeaders {
		if v := r.Header.Get(h); v != "" {
			md.Set(h, v)
		}
	}

	start := time.Now()
	respJSON, err := backend.Invoke(r.Context(), svcMethod, body, md, deadline)
	route := reg.Prefix + "/" + svcMethod
	s.Metrics.Duration.WithLabelValues(route).Observe(time.Since(start).Seconds())
	if err != nil {
		if errors.Is(err, transcode.ErrMethodNotFound) {
			s.count(route, 404)
			writeError(w, 404, "not_found", "unknown method "+svcMethod)
			return
		}
		st := status.Convert(err)
		httpCode := transcode.HTTPStatus(st.Code())
		s.count(route, httpCode)
		writeError(w, httpCode, st.Code().String(), st.Message())
		return
	}
	if len(respJSON) > 4<<20 {
		s.count(route, 500)
		writeError(w, 500, "internal", "response exceeds 4MiB")
		return
	}
	s.count(route, 200)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(respJSON)
}

func (s *Server) count(route string, code int) {
	s.Metrics.Requests.WithLabelValues(route, strconv.Itoa(code)).Inc()
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	channel := r.PathValue("channel")
	rule, ok := s.Store.Get().ChannelRule(channel)
	if !ok {
		writeError(w, 404, "not_found", "unknown channel")
		return
	}
	if _, aerr := auth.Authorize(s.Verifier, rule, r.Header.Get("Authorization")); aerr != nil {
		writeError(w, aerr.Status, aerr.Code, aerr.Message)
		return
	}
	if s.RabbitConnected == nil || !s.RabbitConnected() {
		writeError(w, 503, "unavailable", "event stream temporarily unavailable")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, 500, "internal", "streaming unsupported")
		return
	}

	sub := s.Hub.Subscribe(channel)
	defer sub.Close()
	s.Metrics.SSEClients.WithLabelValues(channel).Inc()
	defer s.Metrics.SSEClients.WithLabelValues(channel).Dec()

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	heartbeat := time.NewTicker(sseHeartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			if _, err := w.Write([]byte(": ping\n\n")); err != nil {
				return
			}
			flusher.Flush()
		case data, open := <-sub.C:
			if !open {
				return // evicted or broker down
			}
			if _, err := w.Write([]byte("data: " + string(data) + "\n\n")); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/httpserver/ -race -v`
Expected: PASS (TestTranscodeRoutes, TestCORS, TestHealthz, TestSSE, TestSSE_RabbitDown, TestBodyLimit).

- [ ] **Step 7: Implement `cmd/api-control-plane/main.go`**

```go
// api-control-plane: the api.algovn.com gateway. See docs/superpowers/specs.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/the-algovn/api-control-plane/internal/auth"
	"github.com/the-algovn/api-control-plane/internal/config"
	"github.com/the-algovn/api-control-plane/internal/httpserver"
	"github.com/the-algovn/api-control-plane/internal/observability"
	"github.com/the-algovn/api-control-plane/internal/push"
	"github.com/the-algovn/api-control-plane/internal/transcode"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	regDir := os.Getenv("REGISTRATIONS_DIR")
	if regDir == "" {
		logger.Error("REGISTRATIONS_DIR is required")
		os.Exit(1)
	}
	issuer := env("ISSUER", "https://id.algovn.com")
	jwksURL := env("JWKS_URL", strings.TrimSuffix(issuer, "/")+"/oauth/v2/keys")
	listenAddr := env("LISTEN_ADDR", ":8080")
	metricsAddr := env("METRICS_ADDR", ":9091")
	corsOrigins := strings.Split(env("CORS_ORIGINS", "https://*.algovn.com"), ",")
	amqpURL := os.Getenv("AMQP_URL")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	promReg := prometheus.NewRegistry()
	promReg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := observability.New(promReg)

	store, err := config.NewStore(regDir)
	if err != nil {
		logger.Error("initial config load failed", "dir", regDir, "err", err)
		os.Exit(1)
	}
	store.OnReloadError = func(error) { metrics.ReloadErrors.Inc() }
	go store.Watch(ctx, logger)

	verifier := auth.NewVerifier(ctx, issuer, jwksURL, logger)

	backends := transcode.NewRegistry(logger)
	defer backends.Close()
	backends.Reconcile(ctx, store.Get().Registrations())
	go func() { // pick up config reloads and late-starting upstreams
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				backends.Reconcile(ctx, store.Get().Registrations())
			}
		}
	}()

	hub := push.NewHub()
	rabbitConnected := func() bool { return false }
	if amqpURL != "" {
		consumer := push.NewConsumer(amqpURL, hub, logger)
		go consumer.Run(ctx)
		rabbitConnected = consumer.Connected
	} else {
		logger.Warn("AMQP_URL not set; /events endpoints will return 503")
	}

	srv := &httpserver.Server{
		Store: store, Verifier: verifier, Backends: backends, Hub: hub,
		RabbitConnected: rabbitConnected, CORSOrigins: corsOrigins,
		Logger: logger, Metrics: metrics,
	}
	api := &http.Server{Addr: listenAddr, Handler: srv.Handler()}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(promReg, promhttp.HandlerOpts{}))
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: metricsMux}

	go func() {
		logger.Info("metrics listening", "addr", metricsAddr)
		if err := metricsSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server failed", "err", err)
		}
	}()
	go func() {
		logger.Info("api listening", "addr", listenAddr)
		if err := api.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			logger.Error("api server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = api.Shutdown(shutdownCtx)
	_ = metricsSrv.Shutdown(shutdownCtx)
	hub.CloseAll()
}
```

- [ ] **Step 8: Verify the whole module builds and all unit tests pass**

```bash
go build ./... && go vet ./... && go test ./... -race
```
Expected: build/vet clean; all tests PASS.

- [ ] **Step 9: Commit**

```bash
git add go.mod go.sum internal/observability/ internal/httpserver/ cmd/api-control-plane/
git commit -m "Assemble HTTP gateway: routing, SSE, metrics, main"
```

---

### Task 10: Demo protos, demo-service binary, Dockerfile, CI, repo publish

**Files:**
- Modify: `protos/algovn/demo/v1/demo.proto` (+ regenerate `protos/gen/go/...`)
- Create: `cmd/demo-service/main.go`
- Create: `Dockerfile`
- Create: `.github/workflows/build.yaml`

**Interfaces:**
- Consumes: protos module `github.com/the-algovn/protos/gen/go` (public repo).
- Produces: image `ghcr.io/the-algovn/api-control-plane:main` containing `/api-control-plane` (entrypoint) and `/demo-service`; `algovn.demo.v1.DemoService` with `Ping` (echo), `WhoAmI` (returns JWT `sub` parsed from forwarded metadata), `AdminPing`; demo publishes `{"message":…}` to channel `demo.ping` when `AMQP_URL` is set.

- [ ] **Step 1: Extend the demo proto**

Edit `/Users/duclm27/the-algovn/protos/algovn/demo/v1/demo.proto` — append after `PingResponse`:

```proto
message WhoAmIRequest {}

message WhoAmIResponse {
  string sub = 1;
}

message AdminPingRequest {}
```

and change the service block to:

```proto
service DemoService {
  rpc Ping(PingRequest) returns (PingResponse);
  // WhoAmI echoes the caller's Zitadel subject, parsed from the forwarded
  // Authorization metadata — smoke-tests the claims-forwarding convention.
  rpc WhoAmI(WhoAmIRequest) returns (WhoAmIResponse);
  // AdminPing exists so the gateway's role:admin rule has a target.
  rpc AdminPing(AdminPingRequest) returns (PingResponse);
}
```

```bash
cd /Users/duclm27/the-algovn/protos
buf lint && buf generate
git add algovn/demo/v1/demo.proto gen/go/algovn/demo/v1/
git commit -m "Add WhoAmI and AdminPing to demo v1"
git push
git tag gen/go/v0.1.0   # nested-module tag: REQUIRED for `go get .../gen/go@v0.1.0`
git push origin gen/go/v0.1.0
```

Expected: lint clean; generate updates `gen/go/algovn/demo/v1/*.pb.go`; tag visible on GitHub.

- [ ] **Step 2: Write `cmd/demo-service/main.go`**

```bash
cd /Users/duclm27/the-algovn/api-control-plane
go get github.com/the-algovn/protos/gen/go@v0.1.0
```

```go
// demo-service: permanent smoke-test tenant behind api-control-plane.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	demov1 "github.com/the-algovn/protos/gen/go/algovn/demo/v1"
)

type server struct {
	demov1.UnimplementedDemoServiceServer
	logger  *slog.Logger
	publish func(channel string, body []byte)
}

func (s *server) Ping(_ context.Context, req *demov1.PingRequest) (*demov1.PingResponse, error) {
	msg := "pong: " + req.GetMessage()
	if s.publish != nil {
		body, _ := json.Marshal(map[string]string{"message": msg})
		s.publish("demo.ping", body)
	}
	return &demov1.PingResponse{Message: msg}, nil
}

// WhoAmI parses the forwarded JWT payload per authnz-conventions.md:
// read-only base64 decode of segment 2 — Kong/the gateway already verified it.
func (s *server) WhoAmI(ctx context.Context, _ *demov1.WhoAmIRequest) (*demov1.WhoAmIResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return nil, status.Error(codes.Unauthenticated, "no authorization metadata")
	}
	parts := strings.Split(strings.TrimPrefix(vals[0], "Bearer "), ".")
	if len(parts) != 3 {
		return nil, status.Error(codes.Unauthenticated, "not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "bad JWT payload")
	}
	var claims struct {
		Sub string `json:"sub"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, status.Error(codes.Unauthenticated, "bad claims")
	}
	return &demov1.WhoAmIResponse{Sub: claims.Sub}, nil
}

func (s *server) AdminPing(context.Context, *demov1.AdminPingRequest) (*demov1.PingResponse, error) {
	return &demov1.PingResponse{Message: "admin pong"}, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &server{logger: logger}
	if url := os.Getenv("AMQP_URL"); url != "" {
		srv.publish = newPublisher(ctx, url, logger)
	}

	lis, err := net.Listen("tcp", ":9090")
	if err != nil {
		logger.Error("listen failed", "err", err)
		os.Exit(1)
	}
	gs := grpc.NewServer()
	demov1.RegisterDemoServiceServer(gs, srv)
	healthpb.RegisterHealthServer(gs, health.NewServer())
	reflection.Register(gs)

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		_ = (&http.Server{Addr: ":9091", Handler: mux}).ListenAndServe()
	}()
	go func() {
		<-ctx.Done()
		gs.GracefulStop()
	}()
	logger.Info("demo-service listening", "addr", ":9090")
	if err := gs.Serve(lis); err != nil {
		logger.Error("serve failed", "err", err)
		os.Exit(1)
	}
}

// newPublisher returns a fire-and-forget AMQP publish func; failures are
// logged, never fatal — events are best-effort by design.
func newPublisher(ctx context.Context, url string, logger *slog.Logger) func(string, []byte) {
	type conn struct {
		ch *amqp.Channel
		c  *amqp.Connection
	}
	var mu sync.Mutex // Ping handlers run concurrently across connections
	var cur *conn
	dial := func() *conn {
		c, err := amqp.Dial(url)
		if err != nil {
			logger.Warn("amqp dial failed", "err", err)
			return nil
		}
		ch, err := c.Channel()
		if err != nil {
			_ = c.Close()
			return nil
		}
		if err := ch.ExchangeDeclare("events", "topic", true, false, false, false, nil); err != nil {
			_ = c.Close()
			return nil
		}
		return &conn{ch: ch, c: c}
	}
	return func(channel string, body []byte) {
		mu.Lock()
		defer mu.Unlock()
		if cur == nil || cur.c.IsClosed() {
			cur = dial()
			if cur == nil {
				return
			}
		}
		err := cur.ch.PublishWithContext(ctx, "events", channel, false, false,
			amqp.Publishing{ContentType: "application/json", Body: body})
		if err != nil {
			logger.Warn("publish failed", "channel", channel, "err", err)
			cur = nil
		}
	}
}
```

- [ ] **Step 3: Verify it builds and serves reflection**

```bash
go build ./... && go vet ./...
go run ./cmd/demo-service &
sleep 1
grpcurl -plaintext localhost:9090 list
grpcurl -plaintext -d '{"message":"x"}' localhost:9090 algovn.demo.v1.DemoService/Ping
kill %1
```
Expected: `list` shows `algovn.demo.v1.DemoService`; Ping returns `{"message":"pong: x"}`. (If `grpcurl` is missing: `brew install grpcurl`.)

- [ ] **Step 4: Write `Dockerfile`**

```dockerfile
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/api-control-plane ./cmd/api-control-plane \
 && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/demo-service ./cmd/demo-service

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/api-control-plane /api-control-plane
COPY --from=build /out/demo-service /demo-service
ENTRYPOINT ["/api-control-plane"]
```

Verify: `podman build -t acp-test .` (needs `podman machine start`). Expected: image builds.

- [ ] **Step 5: Write `.github/workflows/build.yaml`** (iac template + a test job)

```yaml
name: build
on:
  push:
    branches: [main]
    tags: ["v*.*.*"]
permissions:
  contents: read
  packages: write
jobs:
  test:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version-file: go.mod }
      - run: go vet ./...
      - run: go test ./... -race
  build:
    needs: test
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - uses: docker/metadata-action@v5
        id: meta
        with:
          images: ghcr.io/${{ github.repository }}
          tags: |
            type=semver,pattern={{version}}
            type=sha,prefix=sha-
            type=raw,value=main,enable={{is_default_branch}}
      - uses: docker/build-push-action@v6
        with:
          context: .
          platforms: linux/amd64
          push: true
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
```

> Deviation from the template: adds the `test` gate and a `main` tag on default-branch pushes (the deployment references `:main`, like showcase).

- [ ] **Step 6: Commit, publish the repo, verify CI**

```bash
git add cmd/demo-service/ Dockerfile .github/
git commit -m "Add demo-service binary, Dockerfile, and CI workflow"
gh repo create the-algovn/api-control-plane --public --source . --push
gh run watch --repo the-algovn/api-control-plane --exit-status
```

Expected: repo visible, workflow green, package `ghcr.io/the-algovn/api-control-plane` exists.

- [ ] **Step 7: Make the GHCR package public**

GitHub → org `the-algovn` → Packages → `api-control-plane` → Package settings → Change visibility → Public. (Same as showcase; no pull secret needed then.)

Verify: `podman pull ghcr.io/the-algovn/api-control-plane:main` succeeds without login.

---

### Task 11: iac — deploy api-control-plane + demo-service

**Files:**
- Create: `iac/apps/api-control-plane/{namespace,deployment,service,ingress,vmservicescrape,kustomization}.yaml`
- Create: `iac/apps/api-control-plane/amqp-creds-sealed.yaml` (kubeseal)
- Create: `iac/apps/api-control-plane/registrations/demo.yaml`
- Create: `iac/apps/demo-service/{namespace,deployment,service,vmservicescrape,kustomization}.yaml`
- Create: `iac/apps/demo-service/amqp-creds-sealed.yaml` (kubeseal)
- Create: `iac/clusters/algovn/apps/api-control-plane.yaml`, `iac/clusters/algovn/apps/demo-service.yaml`
- Create: `iac/platform/image-updater/api-control-plane-updater.yaml`, `iac/platform/image-updater/demo-service-updater.yaml`
- Modify: `iac/platform/image-updater/kustomization.yaml` (add the two new files to `resources:`)

**Interfaces:**
- Consumes: image from Task 10, RabbitMQ + password from Task 1 (password manager entry `rabbitmq-events`).
- Produces: `https://api.algovn.com` live; upstream `demo-service.demo-service.svc.cluster.local:9090`.

- [ ] **Step 1: Seal the AMQP creds for both namespaces** (double-seal pattern)

```bash
cd /Users/duclm27/the-algovn/iac
RABBIT_PASS='<from password manager: rabbitmq-events>'
AMQP_URL="amqp://events:${RABBIT_PASS}@rabbitmq.rabbitmq.svc.cluster.local:5672/"
for ns in api-control-plane demo-service; do
  kubectl create secret generic amqp-creds -n "$ns" --from-literal=url="$AMQP_URL" \
    --dry-run=client -o yaml | kubeseal --context algovn-remote --format yaml \
    > "apps/$ns/amqp-creds-sealed.yaml"
done
```

(Create the `apps/api-control-plane` and `apps/demo-service` directories first: `mkdir -p apps/api-control-plane/registrations apps/demo-service`.)

- [ ] **Step 2: api-control-plane manifests**

`apps/api-control-plane/namespace.yaml`:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: api-control-plane
```

`apps/api-control-plane/registrations/demo.yaml`:

```yaml
prefix: /demo
upstream: dns:///demo-service.demo-service.svc.cluster.local:9090
defaultRule: authenticated
routes:
  - method: algovn.demo.v1.DemoService/Ping
    rule: anonymous
  - method: algovn.demo.v1.DemoService/WhoAmI
    rule: authenticated
  - method: algovn.demo.v1.DemoService/AdminPing
    rule: role:admin
channels:
  - name: demo.ping
    rule: anonymous
```

`apps/api-control-plane/deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api-control-plane
  namespace: api-control-plane
spec:
  replicas: 1
  strategy:
    type: RollingUpdate
    rollingUpdate: { maxSurge: 1, maxUnavailable: 0 }
  selector:
    matchLabels: { app: api-control-plane }
  template:
    metadata:
      labels: { app: api-control-plane }
    spec:
      containers:
        - name: api-control-plane
          image: ghcr.io/the-algovn/api-control-plane:main
          ports:
            - { containerPort: 8080, name: http }
            - { containerPort: 9091, name: metrics }
          env:
            - { name: REGISTRATIONS_DIR, value: /etc/api-registrations }
            - { name: ISSUER, value: "https://id.algovn.com" }
            - { name: CORS_ORIGINS, value: "https://*.algovn.com" }
            - name: AMQP_URL
              valueFrom: { secretKeyRef: { name: amqp-creds, key: url } }
          readinessProbe:
            httpGet: { path: /healthz, port: 8080 }
            initialDelaySeconds: 3
            periodSeconds: 10
          livenessProbe:
            httpGet: { path: /healthz, port: 8080 }
            initialDelaySeconds: 10
            periodSeconds: 20
          resources:
            requests: { cpu: 50m, memory: 64Mi }
            limits: { memory: 192Mi }
          volumeMounts:
            - { name: registrations, mountPath: /etc/api-registrations, readOnly: true }
      volumes:
        - name: registrations
          configMap: { name: api-registrations }
```

`apps/api-control-plane/service.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: api-control-plane
  namespace: api-control-plane
  labels: { app: api-control-plane }   # VMServiceScrape selects Services by THEIR labels
spec:
  selector: { app: api-control-plane }
  ports:
    - { port: 80, targetPort: 8080, name: http }
    - { port: 9091, targetPort: 9091, name: metrics }
```

`apps/api-control-plane/ingress.yaml` (NO jwt-auth plugin — the control plane is the auth gate; buffering off for SSE):

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: api-control-plane
  namespace: api-control-plane
  annotations:
    konghq.com/response-buffering: "false"
spec:
  ingressClassName: kong
  rules:
    - host: api.algovn.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: api-control-plane
                port: { number: 80 }
  tls:
    - hosts: [api.algovn.com]
```

`apps/api-control-plane/vmservicescrape.yaml`:

```yaml
apiVersion: operator.victoriametrics.com/v1beta1
kind: VMServiceScrape
metadata:
  name: api-control-plane
  namespace: monitoring
spec:
  namespaceSelector: { matchNames: [api-control-plane] }
  selector:
    matchLabels: { app: api-control-plane }
  endpoints:
    - port: metrics
```

`apps/api-control-plane/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - namespace.yaml
  - amqp-creds-sealed.yaml
  - deployment.yaml
  - service.yaml
  - ingress.yaml
  - vmservicescrape.yaml
configMapGenerator:
  - name: api-registrations
    namespace: api-control-plane
    files:
      - registrations/demo.yaml
generatorOptions:
  disableNameSuffixHash: true   # stable name => ConfigMap updates hot-reload in place, no pod restart
images:
  - name: ghcr.io/the-algovn/api-control-plane
    newTag: main
```

- [ ] **Step 3: demo-service manifests** (from `templates/grpc-service`)

`apps/demo-service/namespace.yaml`:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: demo-service
```

`apps/demo-service/deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: demo-service
  namespace: demo-service
spec:
  replicas: 1
  selector:
    matchLabels: { app: demo-service }
  template:
    metadata:
      labels: { app: demo-service }
    spec:
      containers:
        - name: demo-service
          image: ghcr.io/the-algovn/api-control-plane:main
          command: ["/demo-service"]
          ports:
            - { containerPort: 9090, name: grpc }
            - { containerPort: 9091, name: metrics }
          env:
            - name: AMQP_URL
              valueFrom: { secretKeyRef: { name: amqp-creds, key: url } }
          readinessProbe:
            grpc: { port: 9090 }
            initialDelaySeconds: 5
          livenessProbe:
            grpc: { port: 9090 }
            initialDelaySeconds: 10
          resources:
            requests: { cpu: 25m, memory: 64Mi }
            limits: { memory: 128Mi }
```

`apps/demo-service/service.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: demo-service
  namespace: demo-service
  labels: { app: demo-service }   # VMServiceScrape selects Services by THEIR labels
spec:
  clusterIP: None          # headless: enables grpc-go dns:/// round_robin later
  selector: { app: demo-service }
  ports:
    - { port: 9090, targetPort: 9090, name: grpc }
    - { port: 9091, targetPort: 9091, name: metrics }
```

`apps/demo-service/vmservicescrape.yaml`:

```yaml
apiVersion: operator.victoriametrics.com/v1beta1
kind: VMServiceScrape
metadata:
  name: demo-service
  namespace: monitoring
spec:
  namespaceSelector: { matchNames: [demo-service] }
  selector:
    matchLabels: { app: demo-service }
  endpoints:
    - port: metrics
```

`apps/demo-service/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - namespace.yaml
  - amqp-creds-sealed.yaml
  - deployment.yaml
  - service.yaml
  - vmservicescrape.yaml
images:
  - name: ghcr.io/the-algovn/api-control-plane
    newTag: main
```

- [ ] **Step 4: Argo Applications + image updaters**

`clusters/algovn/apps/api-control-plane.yaml`:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: api-control-plane
  namespace: argocd
  annotations:
    argocd.argoproj.io/sync-wave: "1"
spec:
  project: default
  source:
    repoURL: https://github.com/the-algovn/iac.git
    targetRevision: main
    path: apps/api-control-plane
  destination:
    server: https://kubernetes.default.svc
    namespace: api-control-plane
  syncPolicy:
    automated: { prune: true, selfHeal: true }
```

`clusters/algovn/apps/demo-service.yaml`: same shape with `name: demo-service`, `path: apps/demo-service`, `namespace: demo-service`.

`platform/image-updater/api-control-plane-updater.yaml`:

```yaml
apiVersion: argocd-image-updater.argoproj.io/v1alpha1
kind: ImageUpdater
metadata:
  name: api-control-plane-updater
  namespace: argocd
spec:
  applicationRefs:
    - namePattern: api-control-plane
      images:
        - alias: api-control-plane
          imageName: ghcr.io/the-algovn/api-control-plane:main
          commonUpdateSettings:
            updateStrategy: digest
            pullSecret: pullsecret:argocd/ghcr-creds
          manifestTargets:
            kustomize:
              name: ghcr.io/the-algovn/api-control-plane
      writeBackConfig:
        method: git:secret:argocd/git-creds
        gitConfig:
          branch: main
          writeBackTarget: kustomization:.
```

`platform/image-updater/demo-service-updater.yaml`: same shape with `name: demo-service-updater`, `namePattern: demo-service`, `alias: demo-service`.

Edit `platform/image-updater/kustomization.yaml`: append `- api-control-plane-updater.yaml` and `- demo-service-updater.yaml` to its `resources:` list.

- [ ] **Step 5: Validate, commit, push, verify**

```bash
cd /Users/duclm27/the-algovn/iac
./scripts/validate.sh
git add apps/api-control-plane apps/demo-service clusters/algovn/apps/api-control-plane.yaml \
        clusters/algovn/apps/demo-service.yaml platform/image-updater/
git commit -m "Deploy api-control-plane and demo-service behind api.algovn.com"
git push
kubectl --context algovn-remote -n api-control-plane rollout status deploy/api-control-plane --timeout=300s
kubectl --context algovn-remote -n demo-service rollout status deploy/demo-service --timeout=300s
kubectl --context algovn-remote -n api-control-plane logs deploy/api-control-plane | tail -20
```

Expected: both rollouts complete; logs show `jwks loaded`, `rabbitmq connected`, `backend ready prefix=/demo`, `api listening`. DNS + tunnel for `api.algovn.com` appear automatically (external-dns watches the Ingress) — allow a couple of minutes.

---

### Task 12: Acceptance runbook + conventions docs + spec amendment

**Files:**
- Create: `iac/docs/runbooks/api-control-plane.md`
- Create: `iac/docs/api-conventions.md`
- Modify: `iac/docs/authnz-conventions.md` (edge-gate section)
- Modify: `iac/README.md` (Status)
- Modify: `api-control-plane/docs/superpowers/specs/2026-07-13-api-control-plane-design.md` (amendments)

- [ ] **Step 1: Run the acceptance checks against the live cluster**

```bash
# anonymous route
curl -s https://api.algovn.com/demo/algovn.demo.v1.DemoService/Ping \
  -H 'content-type: application/json' -d '{"message":"hello"}'
# -> {"message":"pong: hello"}                                      (200)

# authenticated route without token
curl -s -o /dev/null -w '%{http_code}\n' \
  https://api.algovn.com/demo/algovn.demo.v1.DemoService/WhoAmI -d '{}'
# -> 401

# with a real Zitadel JWT access token (any app with Token Type JWT — see
# docs/runbooks/zitadel.md; quickest is a service user with client credentials)
TOKEN='<zitadel access token>'
curl -s https://api.algovn.com/demo/algovn.demo.v1.DemoService/WhoAmI \
  -H "Authorization: Bearer $TOKEN" -d '{}'
# -> {"sub":"<your user id>"}                                        (200)

# role-gated route with a non-admin token
curl -s -o /dev/null -w '%{http_code}\n' \
  https://api.algovn.com/demo/algovn.demo.v1.DemoService/AdminPing \
  -H "Authorization: Bearer $TOKEN" -d '{}'
# -> 403

# SSE end-to-end: terminal 1 stays open, terminal 2 triggers a Ping
curl -N https://api.algovn.com/events/demo.ping        # terminal 1
curl -s https://api.algovn.com/demo/algovn.demo.v1.DemoService/Ping -d '{"message":"sse"}'  # terminal 2
# terminal 1 -> data: {"message":"pong: sse"}
```

Expected: every command returns the annotated result. If SSE stalls, check Kong response buffering annotation and `kubectl -n api-control-plane logs` for `rabbitmq connected`.

- [ ] **Step 2: Write `iac/docs/runbooks/api-control-plane.md`**

Content: the exact command transcript from Step 1 (with expected outputs), plus operational notes:
- Route changes: edit `apps/api-control-plane/registrations/*.yaml`, PR, Argo sync — the pod hot-reloads within ~1 min (kubelet ConfigMap sync), no restart. A broken file keeps the last good config and increments `acp_config_reload_errors_total`.
- New upstream after deploy: descriptors retry every 30s; `acp_requests_total{code="502"}` spikes mean the upstream is down or lacks reflection.
- RabbitMQ default user is only created on first boot; to rotate: `kubectl -n rabbitmq exec rabbitmq-0 -- rabbitmqctl change_password events '<new>'`, then reseal `amqp-creds` in `api-control-plane` and `demo-service` namespaces and update the password manager (`rabbitmq-events`).
- Metrics: `acp_*` series in VictoriaMetrics; SSE gauge = `acp_sse_clients`.

- [ ] **Step 3: Write `iac/docs/api-conventions.md`**

```markdown
# API conventions — api.algovn.com

Spec: api-control-plane repo, docs/superpowers/specs/2026-07-13-api-control-plane-design.md.
Every product API lives under `https://api.algovn.com/<prefix>/…`, served by
`api-control-plane` (Kong routes the host with NO jwt-auth plugin; the control
plane verifies Zitadel JWTs itself — see authnz-conventions.md).

## Calling an API
`POST /<prefix>/<pkg.Service>/<Method>` with a JSON body (protojson mapping,
≤1 MiB). Errors: `{"code":"<grpc-code>","message":"…"}`; status mapping:
InvalidArgument→400, Unauthenticated→401, PermissionDenied→403, NotFound→404,
Unavailable→502, DeadlineExceeded→504, else 500.

## Registering a product API
Add `apps/api-control-plane/registrations/<product>.yaml` in THIS repo (PR-reviewed,
hot-reloaded — no gateway restart):

    prefix: /<product>              # single lowercase segment
    upstream: dns:///<svc>.<ns>.svc.cluster.local:9090
    defaultRule: authenticated      # anonymous | authenticated | role:<r>
    routes:
      - method: algovn.<product>.v1.<Service>/<Method>
        rule: anonymous
        deadline: 3s                # optional, default 5s
    channels:
      - name: <product>.<topic>     # SSE channel, same rule vocabulary
        rule: anonymous

Requirements for the upstream: pure gRPC on :9090 h2c with server reflection
enabled (descriptors are fetched via reflection; unary only in v1). The
verified `Authorization` header arrives as gRPC metadata — parse claims per
authnz-conventions.md, never re-verify.

## Realtime push (shared mechanic)
Publish JSON to RabbitMQ topic exchange `events`, routing key = channel name;
body = exactly what browsers receive. Browsers: `new EventSource
('https://api.algovn.com/events/<channel>')`. No replay: snapshots or
fire-and-forget only. Broker creds: seal a copy of `amqp-creds` into your
namespace (double-seal pattern, source `rabbitmq-events` in the password
manager). Go publish example: see `cmd/demo-service/main.go` (newPublisher)
in the api-control-plane repo.
```

- [ ] **Step 4: Edit `iac/docs/authnz-conventions.md`**

In the "Protecting a route (edge gate)" section, append:

```markdown
**Exception — api.algovn.com:** this host is routed to `api-control-plane`
WITHOUT Kong's jwt-auth plugin. The control plane verifies RS256 signatures
against Zitadel's JWKS (auto-refreshed — no committed public key, no manual
rotation) and enforces per-route rules (anonymous / authenticated / role:<r>)
from GitOps registration files. Upstream services behind it keep the same
contract: parse the forwarded token payload, never re-verify. See
docs/api-conventions.md.
```

- [ ] **Step 5: Update `iac/README.md` Status and commit iac docs**

Append to the Status section:

```markdown
api.algovn.com gateway live (api-control-plane + RabbitMQ events bus + demo-service tenant) — <today's date>, spec in the-algovn/api-control-plane repo, conventions docs/api-conventions.md, runbook docs/runbooks/api-control-plane.md.
```

```bash
cd /Users/duclm27/the-algovn/iac
./scripts/validate.sh
git add docs/api-conventions.md docs/authnz-conventions.md docs/runbooks/api-control-plane.md README.md
git commit -m "Document api.algovn.com gateway conventions and runbook"
git push
```

- [ ] **Step 6: Amend the spec and commit (api-control-plane repo)**

Append to `docs/superpowers/specs/2026-07-13-api-control-plane-design.md`:

```markdown
## Amendments (implementation)

- Registration files live in **iac** (`apps/api-control-plane/registrations/`),
  not this repo — iac is the GitOps source of truth and the ConfigMap is
  generated there (stable name, hot-reload in place).
- demo-service ships as a second binary in this repo/image (`cmd/demo-service`,
  `command: ["/demo-service"]`) rather than its own repo — it is a permanent
  smoke tenant, not a product.
- demo.proto gained `WhoAmI` (claims-forwarding check) and `AdminPing`
  (role-rule target); protos tag `gen/go/v0.1.0`.
- Push gateway metrics listener is `:9091`, never exposed via the Ingress —
  `/metrics` on the public listener does not exist (the reserved-prefix rule
  still blocks registrations from claiming it).
```

```bash
cd /Users/duclm27/the-algovn/api-control-plane
git add docs/superpowers/specs/2026-07-13-api-control-plane-design.md
git commit -m "Record implementation amendments in design spec"
git push
```

---

## Post-plan Self-Review (done at planning time)

- **Spec coverage:** §1 goal→Tasks 9/11; §4.1 registry→2, hot reload→3; §4.2 auth→4; §4.3 transcoder→5/6; §4.4 push→7/8; §4.5 observability→9; §5 schema→2 (validation) + 11 (demo.yaml); §6 flows/CORS/limits→9; §7 failure modes→3 (reload), 4 (JWKS retry), 5 (descriptor retry via reconcile), 7 (slow client), 8 (reconnect); §8 testing→each task + 12 (acceptance); §9 rollout→1, 10, 11, 12 (docs incl. authnz edit + api-conventions); §10 out of scope — nothing in plan exceeds it.
- **Type consistency:** `config.Store.Get() *Snapshot`, `Registrations() []*Registration`, `auth.Authorize(v, rule, header) (Identity, *AuthError)`, `transcode.Registry.Reconcile(ctx, []*config.Registration)`, `Backend.Invoke(ctx, method, body, md, deadline)`, `push.Hub` API — cross-checked against every consumer (Tasks 9 tests compile against Tasks 2–8 signatures).
- **Placeholders:** none — every code step is complete; Task 12 Step 2 describes runbook content as a transcript of Step 1 commands (intentional: outputs come from the live run).
