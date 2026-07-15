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
    verb: GET
    path: /ping
    rule: anonymous
  - method: algovn.demo.v1.DemoService/AdminPing
    verb: POST
    path: /admin/ping
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

	m, ok := snap.Route("GET", "/demo/ping")
	require.True(t, ok)
	require.Equal(t, "/demo", m.Prefix)
	require.Equal(t, "algovn.demo.v1.DemoService/Ping", m.GRPCMethod)
	require.Equal(t, "anonymous", m.Rule)
	require.Equal(t, DefaultDeadline, m.Deadline)
	require.Equal(t, "/demo/ping", m.Metric)

	m, ok = snap.Route("POST", "/demo/admin/ping")
	require.True(t, ok)
	require.Equal(t, "role:admin", m.Rule)
	require.Equal(t, 3*time.Second, m.Deadline)

	// wrong verb on a known path is not a match, but the path is known
	_, ok = snap.Route("POST", "/demo/ping")
	require.False(t, ok)
	require.Equal(t, []string{"GET"}, snap.PathVerbs("/demo/ping"))

	// unknown path: no match, no verbs
	_, ok = snap.Route("GET", "/demo/nope")
	require.False(t, ok)
	require.Empty(t, snap.PathVerbs("/demo/nope"))

	cr, ok := snap.ChannelRule("demo.ping")
	require.True(t, ok)
	require.Equal(t, "anonymous", cr)
	_, ok = snap.ChannelRule("nope.nope")
	require.False(t, ok)
}

func TestLoadDir_UpstreamSchemeNormalized(t *testing.T) {
	dir := t.TempDir()
	writeReg(t, dir, "a.yaml", "prefix: /a\nupstream: svc.ns.svc:9090\n")
	snap, err := LoadDir(dir)
	require.NoError(t, err)
	regs := snap.Registrations()
	require.Len(t, regs, 1)
	require.Equal(t, "dns:///svc.ns.svc:9090", regs[0].Upstream)
	// empty defaultRule defaults to authenticated
	require.Equal(t, "authenticated", regs[0].DefaultRule)
}

func TestLoadDir_Invalid(t *testing.T) {
	cases := map[string]string{
		"reserved prefix":  "prefix: /events\nupstream: s:9090\n",
		"bad prefix":       "prefix: /Two/Seg\nupstream: s:9090\n",
		"missing upstream": "prefix: /a\n",
		"bad rule":         "prefix: /a\nupstream: s:9090\ndefaultRule: sometimes\n",
		"bad role rule":    "prefix: /a\nupstream: s:9090\nroutes: [{method: p.S/M, verb: GET, path: /x, rule: 'role:'}]\n",
		"bad channel name": "prefix: /a\nupstream: s:9090\nchannels: [{name: nodot, rule: anonymous}]\n",
		"bad yaml":         "prefix: [unclosed\n",
		"missing verb":     "prefix: /a\nupstream: s:9090\nroutes: [{method: p.S/M, path: /x, rule: anonymous}]\n",
		"bad verb":         "prefix: /a\nupstream: s:9090\nroutes: [{method: p.S/M, verb: FETCH, path: /x, rule: anonymous}]\n",
		"missing path":     "prefix: /a\nupstream: s:9090\nroutes: [{method: p.S/M, verb: GET, rule: anonymous}]\n",
		"bad path":         "prefix: /a\nupstream: s:9090\nroutes: [{method: p.S/M, verb: GET, path: /Bad_Seg, rule: anonymous}]\n",
		"trailing slash":   "prefix: /a\nupstream: s:9090\nroutes: [{method: p.S/M, verb: GET, path: /x/, rule: anonymous}]\n",
		"dup verb+path":    "prefix: /a\nupstream: s:9090\nroutes: [{method: p.S/M, verb: GET, path: /x, rule: anonymous}, {method: p.S/N, verb: GET, path: /x, rule: anonymous}]\n",
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

func TestLoadDir_VerbNormalizedAndSameMethodTwoPaths(t *testing.T) {
	dir := t.TempDir()
	// lowercase verb is normalized; one gRPC method may back two distinct routes
	writeReg(t, dir, "a.yaml", "prefix: /a\nupstream: s:9090\nroutes:\n"+
		"  - {method: p.S/Get, verb: get, path: /thing, rule: anonymous}\n"+
		"  - {method: p.S/Get, verb: get, path: /thing-alias, rule: anonymous}\n")
	snap, err := LoadDir(dir)
	require.NoError(t, err)
	_, ok := snap.Route("GET", "/a/thing")
	require.True(t, ok)
	_, ok = snap.Route("GET", "/a/thing-alias")
	require.True(t, ok)
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
