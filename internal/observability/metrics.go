// Package observability defines the service's Prometheus metrics.
package observability

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	Requests     *prometheus.CounterVec
	Duration     *prometheus.HistogramVec
	SSEClients   *prometheus.GaugeVec
	ReloadErrors prometheus.Counter
}

func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "acp_requests_total", Help: "API requests by registered route and HTTP code.",
		}, []string{"route", "code"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "acp_request_duration_seconds", Help: "API request latency by registered route.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route"}),
		SSEClients: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "acp_sse_clients", Help: "Connected SSE clients per channel.",
		}, []string{"channel"}),
		ReloadErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "acp_config_reload_errors_total", Help: "Failed registration reloads.",
		}),
	}
	reg.MustRegister(m.Requests, m.Duration, m.SSEClients, m.ReloadErrors)
	return m
}
