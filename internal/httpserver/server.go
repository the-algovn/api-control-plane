// Package httpserver assembles routing, auth, transcoding and SSE.
package httpserver

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
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
	SSEMaxConns     int // global cap on concurrent SSE connections; 0 = unlimited
	Logger          *slog.Logger
	Metrics         *observability.Metrics

	sseConns atomic.Int64
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
	entry, ok := snap.Route(r.Method, r.URL.Path)
	if !ok {
		if verbs := snap.PathVerbs(r.URL.Path); len(verbs) > 0 {
			w.Header().Set("Allow", strings.Join(verbs, ", "))
			writeError(w, 405, "method_not_allowed", "method not allowed for this path")
			return
		}
		writeError(w, 404, "not_found", "unknown path")
		return
	}

	if _, aerr := auth.Authorize(s.Verifier, entry.Rule, r.Header.Get("Authorization")); aerr != nil {
		s.count(entry.Metric, aerr.Status)
		writeError(w, aerr.Status, aerr.Code, aerr.Message)
		return
	}

	backend, err := s.Backends.Backend(entry.Prefix)
	if err != nil {
		s.count(entry.Metric, 502)
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
	respJSON, err := backend.Invoke(r.Context(), entry.GRPCMethod, body, md, entry.Deadline)
	s.Metrics.Duration.WithLabelValues(entry.Metric).Observe(time.Since(start).Seconds())
	if err != nil {
		if errors.Is(err, transcode.ErrMethodNotFound) {
			// A registered path points at a method the upstream doesn't expose
			// (config/deploy mismatch, or reflection briefly stale) — server-side,
			// not a client 404.
			s.count(entry.Metric, 502)
			s.Logger.Warn("registered route targets a method the upstream does not expose",
				"path", r.URL.Path, "method", entry.GRPCMethod)
			writeError(w, 502, "unavailable", "upstream does not expose the requested method")
			return
		}
		st := status.Convert(err)
		httpCode := transcode.HTTPStatus(st.Code())
		s.count(entry.Metric, httpCode)
		if httpCode == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", "2") // PoW token stays valid; client backs off
		}
		writeError(w, httpCode, st.Code().String(), st.Message())
		return
	}
	if len(respJSON) > 4<<20 {
		s.count(entry.Metric, 500)
		writeError(w, 500, "internal", "response exceeds 4MiB")
		return
	}
	s.count(entry.Metric, 200)
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
	if s.SSEMaxConns > 0 {
		if n := s.sseConns.Add(1); n > int64(s.SSEMaxConns) {
			s.sseConns.Add(-1)
			writeError(w, 503, "unavailable", "SSE connection limit reached, retry later")
			return
		}
		defer s.sseConns.Add(-1)
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
	// Base reconnect delay for EventSource; the SPA layers full jitter (0-5s)
	// on top so a rolling deploy doesn't cause a reconnect stampede.
	_, _ = io.WriteString(w, "retry: 3000\n\n")
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
