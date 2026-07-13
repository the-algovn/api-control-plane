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
// missing upstreams (failures are logged and retried on the next call),
// closes backends whose prefix disappeared. Call at startup, on config
// reload, and on a ticker to pick up late-starting upstreams.
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
