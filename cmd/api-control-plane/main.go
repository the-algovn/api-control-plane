// api-control-plane: the api.algovn.com gateway. See docs/superpowers/specs.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/the-algovn/api-control-plane/internal/auth"
	"github.com/the-algovn/api-control-plane/internal/config"
	"github.com/the-algovn/api-control-plane/internal/httpserver"
	"github.com/the-algovn/api-control-plane/internal/observability"
	"github.com/the-algovn/api-control-plane/internal/push"
	"github.com/the-algovn/api-control-plane/internal/transcode"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	regDir := os.Getenv("REGISTRATIONS_DIR")
	if regDir == "" {
		logger.Error("REGISTRATIONS_DIR is required")
		os.Exit(1)
	}
	issuer := env("ISSUER", "https://id.algovn.com")
	jwksURL := env("JWKS_URL", strings.TrimSuffix(issuer, "/")+"/oauth/v2/keys")
	listenAddr := env("LISTEN_ADDR", ":8080")
	metricsAddr := env("METRICS_ADDR", ":9091")
	corsOrigins := strings.Split(env("CORS_ORIGINS", "https://*.algovn.com"), ",")
	sseMaxConns, err := strconv.Atoi(env("SSE_MAX_CONNS", "15000"))
	if err != nil || sseMaxConns < 1 {
		logger.Error("SSE_MAX_CONNS must be a positive integer", "value", env("SSE_MAX_CONNS", "15000"))
		os.Exit(1)
	}
	amqpURL := os.Getenv("AMQP_URL")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	promReg := prometheus.NewRegistry()
	promReg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := observability.New(promReg)

	store, err := config.NewStore(regDir)
	if err != nil {
		logger.Error("initial config load failed", "dir", regDir, "err", err)
		os.Exit(1)
	}
	store.OnReloadError = func(error) { metrics.ReloadErrors.Inc() }
	go store.Watch(ctx, logger)

	verifier := auth.NewVerifier(ctx, issuer, jwksURL, logger)

	backends := transcode.NewRegistry(logger)
	defer backends.Close()
	backends.Reconcile(ctx, store.Get().Registrations())
	go func() { // pick up config reloads and late-starting upstreams
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				backends.Reconcile(ctx, store.Get().Registrations())
			}
		}
	}()

	hub := push.NewHub()
	rabbitConnected := func() bool { return false }
	if amqpURL != "" {
		consumer := push.NewConsumer(amqpURL, hub, logger)
		go consumer.Run(ctx)
		rabbitConnected = consumer.Connected
	} else {
		logger.Warn("AMQP_URL not set; /events endpoints will return 503")
	}

	srv := &httpserver.Server{
		Store: store, Verifier: verifier, Backends: backends, Hub: hub,
		RabbitConnected: rabbitConnected, CORSOrigins: corsOrigins,
		SSEMaxConns: sseMaxConns,
		Logger:      logger, Metrics: metrics,
	}
	api := &http.Server{Addr: listenAddr, Handler: srv.Handler()}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(promReg, promhttp.HandlerOpts{}))
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: metricsMux}

	go func() {
		logger.Info("metrics listening", "addr", metricsAddr)
		if err := metricsSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server failed", "err", err)
		}
	}()
	go func() {
		logger.Info("api listening", "addr", listenAddr)
		if err := api.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			logger.Error("api server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = api.Shutdown(shutdownCtx)
	_ = metricsSrv.Shutdown(shutdownCtx)
	hub.CloseAll()
}
