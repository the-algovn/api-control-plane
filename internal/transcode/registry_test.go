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
