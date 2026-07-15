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

func hasPrefix(snap *Snapshot, prefix string) bool {
	for _, r := range snap.Registrations() {
		if r.Prefix == prefix {
			return true
		}
	}
	return false
}

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

	require.False(t, hasPrefix(st.Get(), "/b"))

	// add a second registration -> picked up
	writeReg(t, dir, "b.yaml", "prefix: /b\nupstream: s2:9090\n")
	require.Eventually(t, func() bool {
		return hasPrefix(st.Get(), "/b")
	}, 5*time.Second, 50*time.Millisecond)

	// break a file -> snapshot stays, error callback fires
	writeReg(t, dir, "b.yaml", "prefix: [broken\n")
	require.Eventually(t, func() bool { return reloadErrs.Load() >= 1 }, 5*time.Second, 50*time.Millisecond)
	require.True(t, hasPrefix(st.Get(), "/b"), "last good snapshot must survive a bad reload")
}

func TestNewStore_FailsOnInvalidDir(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bad.yaml"), []byte("prefix: /events\nupstream: s:9090\n"), 0o644))
	_, err := NewStore(dir)
	require.Error(t, err)
}

func TestStore_CloseWithoutWatch(t *testing.T) {
	dir := t.TempDir()
	writeReg(t, dir, "a.yaml", "prefix: /a\nupstream: s1:9090\n")
	st, err := NewStore(dir)
	require.NoError(t, err)
	require.NoError(t, st.Close())
}
