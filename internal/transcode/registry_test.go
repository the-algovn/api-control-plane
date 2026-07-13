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

func TestRegistry_UpstreamChangeSwapsBackend(t *testing.T) {
	addrA := testsvc.StartServer(t)
	addrB := testsvc.StartServer(t)
	r := NewRegistry(testLogger())
	defer r.Close()
	r.Reconcile(context.Background(), []*config.Registration{reg("/test", addrA)})
	b1, err := r.Backend("/test")
	require.NoError(t, err)
	r.Reconcile(context.Background(), []*config.Registration{reg("/test", addrB)})
	b2, err := r.Backend("/test")
	require.NoError(t, err)
	require.NotSame(t, b1, b2, "upstream change must produce a new backend")
	_, err = b2.Method("algovn.testsvc.v1.TestService/Echo")
	require.NoError(t, err)
}

func TestRegistry_ReflectFailureKeepsLastKnown(t *testing.T) {
	addr, stop := testsvc.StartStoppableServer(t)
	r := NewRegistry(testLogger())
	defer r.Close()
	r.Reconcile(context.Background(), []*config.Registration{reg("/test", addr)})
	b, err := r.Backend("/test")
	require.NoError(t, err)
	stop() // upstream goes away
	r.Reconcile(context.Background(), []*config.Registration{reg("/test", addr)})
	b2, err := r.Backend("/test")
	require.NoError(t, err, "backend must survive a failed refresh")
	require.Same(t, b, b2)
	_, err = b2.Method("algovn.testsvc.v1.TestService/Echo")
	require.NoError(t, err, "last-known descriptors must remain")
}
