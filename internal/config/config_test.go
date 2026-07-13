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
