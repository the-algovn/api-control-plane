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
// The fsnotify watcher is armed at construction so no window exists between
// NewStore and Watch where events are lost; Watch consumes and closes it.
type Store struct {
	dir     string
	cur     atomic.Pointer[Snapshot]
	watcher *fsnotify.Watcher

	// OnReloadError is called (if set) when a reload fails; the previous
	// snapshot stays live. Set before calling Watch.
	OnReloadError func(error)
}

func NewStore(dir string) (*Store, error) {
	snap, err := LoadDir(dir)
	if err != nil {
		return nil, err
	}
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := w.Add(dir); err != nil {
		_ = w.Close()
		return nil, err
	}
	st := &Store{dir: dir, watcher: w}
	st.cur.Store(snap)
	return st, nil
}

func (s *Store) Get() *Snapshot { return s.cur.Load() }

// Close releases the directory watcher. Only callers that never start
// Watch need it — Watch closes the watcher itself when ctx is cancelled.
func (s *Store) Close() error { return s.watcher.Close() }

// Watch blocks until ctx is cancelled, reloading the snapshot on directory
// changes (200ms debounce). Watch must be called at most once per Store.
func (s *Store) Watch(ctx context.Context, logger *slog.Logger) {
	w := s.watcher
	defer w.Close()
	var timer *time.Timer
	reload := make(chan struct{}, 1)
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-w.Events:
			if !ok {
				logger.Error("config watcher closed; hot reload disabled, keeping last good config")
				<-ctx.Done()
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
				logger.Error("config watcher closed; hot reload disabled, keeping last good config")
				<-ctx.Done()
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
