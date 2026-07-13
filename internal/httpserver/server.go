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

	// metric label stays bounded by configuration: unlisted methods
	// (including attacker-supplied garbage) share one bucket per prefix
	route := reg.Prefix + "/unmatched"
	if reg.HasRoute(svcMethod) {
		route = reg.Prefix + "/" + svcMethod
	}

	rule, deadline := reg.RouteRule(svcMethod)
	if _, aerr := auth.Authorize(s.Verifier, rule, r.Header.Get("Authorization")); aerr != nil {
		s.count(route, aerr.Status)
		writeError(w, aerr.Status, aerr.Code, aerr.Message)
		return
	}

	backend, err := s.Backends.Backend(reg.Prefix)
	if err != nil {
		s.count(route, 502)
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
			if err := writeSSE(w, data); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// writeSSE frames one event; multi-line payloads become one data: field per
// line (EventSource rejoins them with \n), keeping frames valid.
func writeSSE(w io.Writer, data []byte) error {
	for _, line := range strings.Split(string(data), "\n") {
		if _, err := io.WriteString(w, "data: "+line+"\n"); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, "\n")
	return err
}
