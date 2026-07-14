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
	"github.com/prometheus/client_golang/prometheus/testutil"
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
	srv     *httptest.Server
	jwks    *authtest.JWKS
	hub     *push.Hub
	metrics *observability.Metrics
	server  *Server
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
	metrics := observability.New(prometheus.NewRegistry())
	s := &Server{
		Store: store, Verifier: verifier, Backends: backends, Hub: hub,
		RabbitConnected: func() bool { return rabbitConnected },
		CORSOrigins:     []string{"https://*.algovn.com", "https://algovn.com"},
		Logger:          logger,
		Metrics:         metrics,
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return &fixture{srv: srv, jwks: jwks, hub: hub, metrics: metrics, server: s}
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

func TestRetryAfterOn429(t *testing.T) {
	f := newFixture(t, true)
	// testsvc Fail returns the given gRPC code: 8 = ResourceExhausted -> 429.
	resp := do(t, "POST", f.srv.URL+"/test/algovn.testsvc.v1.TestService/Fail", f.token(t), `{"code":8,"message":"slow down"}`)
	require.Equal(t, 429, resp.StatusCode)
	require.Equal(t, "2", resp.Header.Get("Retry-After"))
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
	require.Contains(t, resp.Header.Get("Access-Control-Allow-Headers"), "X-Request-Id")

	req2, _ := http.NewRequest("OPTIONS", f.srv.URL+"/test/x/y", nil)
	req2.Header.Set("Origin", "https://evil.example")
	req2.Header.Set("Access-Control-Request-Method", "POST")
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	require.Empty(t, resp2.Header.Get("Access-Control-Allow-Origin"))

	// apex origin (the-button SPA at https://algovn.com) needs an exact entry:
	// the *.algovn.com wildcard deliberately does not match the apex.
	req3, _ := http.NewRequest("OPTIONS", f.srv.URL+"/test/algovn.testsvc.v1.TestService/Echo", nil)
	req3.Header.Set("Origin", "https://algovn.com")
	req3.Header.Set("Access-Control-Request-Method", "POST")
	resp3, err := http.DefaultClient.Do(req3)
	require.NoError(t, err)
	defer resp3.Body.Close()
	require.Equal(t, 204, resp3.StatusCode)
	require.Equal(t, "https://algovn.com", resp3.Header.Get("Access-Control-Allow-Origin"))
	require.Equal(t, "Retry-After", resp3.Header.Get("Access-Control-Expose-Headers"))
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
	waitFor := func(want string) {
		t.Helper()
		for {
			select {
			case l := <-lines:
				if l == want {
					return
				}
			case <-deadline:
				t.Fatalf("SSE line %q not received", want)
			}
		}
	}
	waitFor("retry: 3000") // reconnect hint precedes any event
	waitFor(`data: {"total":1}`)
	waitFor("") // blank line terminates the frame

	// multi-line payload: one data: field per line, EventSource rejoins them
	f.hub.Publish("test.events", []byte("{\n  \"total\": 2\n}"))
	waitFor("data: {")
	waitFor(`data:   "total": 2`)
	waitFor("data: }")
	waitFor("") // blank line terminates the frame
}

func TestSSE_RabbitDown(t *testing.T) {
	f := newFixture(t, false)
	resp := do(t, "GET", f.srv.URL+"/events/test.events", "", "")
	require.Equal(t, 503, resp.StatusCode)
}

func TestSSECap(t *testing.T) {
	f := newFixture(t, true)
	f.server.SSEMaxConns = 2
	open := func() *http.Response {
		req, err := http.NewRequest("GET", f.srv.URL+"/events/test.events", nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}
	require.Equal(t, 200, open().StatusCode) // held open by t.Cleanup
	require.Equal(t, 200, open().StatusCode)
	require.Equal(t, 503, open().StatusCode) // third connection over the cap
}

func TestMetricsCardinalityBounded(t *testing.T) {
	f := newFixture(t, true)
	for _, m := range []string{"x.Garbage/One", "x.Garbage/Two", "x.Garbage/Three"} {
		resp := do(t, "POST", f.srv.URL+"/test/"+m, f.token(t), `{}`)
		require.Equal(t, 404, resp.StatusCode)
	}
	series := testutil.CollectAndCount(f.metrics.Requests)
	require.Equal(t, 1, series, "garbage methods must share one <prefix>/unmatched series")
}

func TestBodyLimit(t *testing.T) {
	f := newFixture(t, true)
	big := `{"message":"` + strings.Repeat("x", 1<<20) + `"}` // > 1 MiB
	resp := do(t, "POST", f.srv.URL+"/test/algovn.testsvc.v1.TestService/Echo", "", big)
	require.Equal(t, 413, resp.StatusCode)
}
