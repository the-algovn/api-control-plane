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
	conn     *grpc.ClientConn
	upstream string
	methods  map[string]protoreflect.MethodDescriptor // "pkg.Service/Method"
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

	reconcileMu sync.Mutex

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
// missing upstreams (failures are logged and retried on the next call);
// for a registration whose upstream changed, dials and reflects the new
// address first and only swaps in the new backend on success (on failure
// the old, stale-but-working backend keeps serving); for a registration
// whose upstream is unchanged, re-reflects on the existing connection to
// pick up new/changed RPCs, keeping the last-known descriptors if the
// upstream is briefly unreachable; closes backends whose prefix
// disappeared. Call at startup, on config reload, and on a ticker (~30s)
// so upstream changes and late-starting/newly-added RPCs surface without
// a gateway restart.
// Reconcile calls are serialized internally; concurrent callers queue.
func (r *Registry) Reconcile(ctx context.Context, regs []*config.Registration) {
	r.reconcileMu.Lock()
	defer r.reconcileMu.Unlock()

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
	existing := make(map[string]*Backend, len(r.byPrefix))
	for prefix, b := range r.byPrefix {
		existing[prefix] = b
	}
	r.mu.Unlock()

	for prefix, reg := range desired {
		b, ok := existing[prefix]

		if !ok {
			nb, err := r.dialAndReflect(ctx, reg.Upstream)
			if err != nil {
				r.logger.Warn("backend not ready; will retry on next reconcile",
					"prefix", prefix, "upstream", reg.Upstream, "err", err)
				continue
			}
			r.mu.Lock()
			r.byPrefix[prefix] = nb
			r.mu.Unlock()
			r.logger.Info("backend ready", "prefix", prefix, "methods", len(nb.methods))
			continue
		}

		if b.upstream != reg.Upstream {
			nb, err := r.dialAndReflect(ctx, reg.Upstream)
			if err != nil {
				r.logger.Warn("new upstream unreachable; keeping old backend",
					"prefix", prefix, "old_upstream", b.upstream, "new_upstream", reg.Upstream, "err", err)
				continue
			}
			r.mu.Lock()
			r.byPrefix[prefix] = nb
			r.mu.Unlock()
			_ = b.conn.Close()
			r.logger.Info("backend upstream changed", "prefix", prefix, "upstream", reg.Upstream, "methods", len(nb.methods))
			continue
		}

		methods, err := r.reflect(ctx, b.conn)
		if err != nil {
			r.logger.Debug("reflect refresh failed; keeping last-known descriptors",
				"prefix", prefix, "upstream", reg.Upstream, "err", err)
			continue
		}
		nb := &Backend{conn: b.conn, upstream: b.upstream, methods: methods}
		r.mu.Lock()
		r.byPrefix[prefix] = nb
		r.mu.Unlock()
	}
}

// dialAndReflect dials upstream and fetches its method descriptors via
// reflection, closing the connection if reflection fails.
func (r *Registry) dialAndReflect(ctx context.Context, upstream string) (*Backend, error) {
	conn, err := r.dial(upstream)
	if err != nil {
		return nil, err
	}
	methods, err := r.reflect(ctx, conn)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &Backend{conn: conn, upstream: upstream, methods: methods}, nil
}

func (r *Registry) dial(upstream string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(upstream,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`),
	)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	return conn, nil
}

// reflect fetches unary method descriptors from conn via server reflection.
func (r *Registry) reflect(ctx context.Context, conn *grpc.ClientConn) (map[string]protoreflect.MethodDescriptor, error) {
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rc := grpcreflect.NewClientAuto(rctx, conn)
	defer rc.Reset()

	svcs, err := rc.ListServices()
	if err != nil {
		return nil, fmt.Errorf("reflection ListServices: %w", err)
	}
	methods := map[string]protoreflect.MethodDescriptor{}
	for _, s := range svcs {
		if strings.HasPrefix(s, "grpc.") {
			continue // reflection/health plumbing
		}
		sd, err := rc.ResolveService(s)
		if err != nil {
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
		return nil, errors.New("no unary methods exposed via reflection")
	}
	return methods, nil
}

func (r *Registry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for prefix, b := range r.byPrefix {
		_ = b.conn.Close()
		delete(r.byPrefix, prefix)
	}
}
