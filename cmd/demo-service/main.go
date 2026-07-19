// demo-service: permanent smoke-test tenant behind api-control-plane.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	demov1 "github.com/the-algovn/protos/gen/go/algovn/demo/v1"
)

type server struct {
	demov1.UnimplementedDemoServiceServer
	logger *slog.Logger
}

func (s *server) Ping(_ context.Context, req *demov1.PingRequest) (*demov1.PingResponse, error) {
	msg := "pong: " + req.GetMessage()
	return &demov1.PingResponse{Message: msg}, nil
}

// WhoAmI parses the forwarded JWT payload per authnz-conventions.md:
// read-only base64 decode of segment 2 — Kong/the gateway already verified it.
func (s *server) WhoAmI(ctx context.Context, _ *demov1.WhoAmIRequest) (*demov1.WhoAmIResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return nil, status.Error(codes.Unauthenticated, "no authorization metadata")
	}
	parts := strings.Split(strings.TrimPrefix(vals[0], "Bearer "), ".")
	if len(parts) != 3 {
		return nil, status.Error(codes.Unauthenticated, "not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "bad JWT payload")
	}
	var claims struct {
		Sub string `json:"sub"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, status.Error(codes.Unauthenticated, "bad claims")
	}
	return &demov1.WhoAmIResponse{Sub: claims.Sub}, nil
}

func (s *server) AdminPing(context.Context, *demov1.AdminPingRequest) (*demov1.PingResponse, error) {
	return &demov1.PingResponse{Message: "admin pong"}, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &server{logger: logger}

	lis, err := net.Listen("tcp", ":9090")
	if err != nil {
		logger.Error("listen failed", "err", err)
		os.Exit(1)
	}
	gs := grpc.NewServer()
	demov1.RegisterDemoServiceServer(gs, srv)
	healthpb.RegisterHealthServer(gs, health.NewServer())
	reflection.Register(gs)

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		_ = (&http.Server{Addr: ":9091", Handler: mux}).ListenAndServe()
	}()
	go func() {
		<-ctx.Done()
		gs.GracefulStop()
	}()
	logger.Info("demo-service listening", "addr", ":9090")
	if err := gs.Serve(lis); err != nil {
		logger.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
