// Package testsvc is a test-only gRPC upstream with server reflection.
package testsvc

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	testsvcv1 "github.com/the-algovn/api-control-plane/internal/testsvc/gen"
)

type server struct {
	testsvcv1.UnimplementedTestServiceServer
}

func (server) Echo(_ context.Context, req *testsvcv1.EchoRequest) (*testsvcv1.EchoResponse, error) {
	return &testsvcv1.EchoResponse{Message: req.GetMessage(), Number: req.GetNumber()}, nil
}

// Fail returns a status error with the given code. Note: code 0 (OK)
// yields a SUCCESSFUL empty response, not an error — grpc treats
// status.Error(codes.OK, …) as nil. Tests exploit this deliberately.
func (server) Fail(_ context.Context, req *testsvcv1.FailRequest) (*testsvcv1.EchoResponse, error) {
	return nil, status.Error(codes.Code(req.GetCode()), req.GetMessage())
}

func (server) Slow(ctx context.Context, req *testsvcv1.SlowRequest) (*testsvcv1.EchoResponse, error) {
	select {
	case <-time.After(time.Duration(req.GetDelayMs()) * time.Millisecond):
		return &testsvcv1.EchoResponse{Message: "done"}, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

// StartServer runs the fixture on a random localhost port and returns its address.
func StartServer(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	testsvcv1.RegisterTestServiceServer(s, server{})
	reflection.Register(s)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}
