package transcode

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/the-algovn/api-control-plane/internal/config"
	"github.com/the-algovn/api-control-plane/internal/testsvc"
)

func readyBackend(t *testing.T) *Backend {
	t.Helper()
	addr := testsvc.StartServer(t)
	r := NewRegistry(testLogger())
	t.Cleanup(r.Close)
	r.Reconcile(context.Background(), []*config.Registration{reg("/test", addr)})
	b, err := r.Backend("/test")
	require.NoError(t, err)
	return b
}

func TestInvoke_Echo(t *testing.T) {
	b := readyBackend(t)
	out, err := b.Invoke(context.Background(), "algovn.testsvc.v1.TestService/Echo",
		[]byte(`{"message":"hi","number":7}`), metadata.MD{}, time.Second)
	require.NoError(t, err)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(out, &resp))
	require.Equal(t, "hi", resp["message"])
	require.EqualValues(t, 7, resp["number"])
}

func TestInvoke_EmptyBody(t *testing.T) {
	b := readyBackend(t)
	out, err := b.Invoke(context.Background(), "algovn.testsvc.v1.TestService/Echo",
		nil, metadata.MD{}, time.Second)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(out))
}

func TestInvoke_BadJSON(t *testing.T) {
	b := readyBackend(t)
	_, err := b.Invoke(context.Background(), "algovn.testsvc.v1.TestService/Echo",
		[]byte(`{"nope":true}`), metadata.MD{}, time.Second)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
}

func TestInvoke_UpstreamError(t *testing.T) {
	b := readyBackend(t)
	_, err := b.Invoke(context.Background(), "algovn.testsvc.v1.TestService/Fail",
		[]byte(`{"code":5,"message":"gone"}`), metadata.MD{}, time.Second)
	st, _ := status.FromError(err)
	require.Equal(t, codes.NotFound, st.Code())
	require.Equal(t, "gone", st.Message())
}

func TestInvoke_Deadline(t *testing.T) {
	b := readyBackend(t)
	_, err := b.Invoke(context.Background(), "algovn.testsvc.v1.TestService/Slow",
		[]byte(`{"delayMs":"2000"}`), metadata.MD{}, 100*time.Millisecond)
	st, _ := status.FromError(err)
	require.Equal(t, codes.DeadlineExceeded, st.Code())
}

func TestInvoke_UnknownMethod(t *testing.T) {
	b := readyBackend(t)
	_, err := b.Invoke(context.Background(), "algovn.testsvc.v1.TestService/Nope",
		nil, metadata.MD{}, time.Second)
	require.ErrorIs(t, err, ErrMethodNotFound)
}

func TestHTTPStatus(t *testing.T) {
	cases := map[codes.Code]int{
		codes.NotFound:         404,
		codes.PermissionDenied: 403,
		codes.InvalidArgument:  400,
		codes.Unavailable:      502,
		codes.DeadlineExceeded: 504,
		codes.Unauthenticated:  401,
		codes.Internal:         500,
		codes.Unknown:          500,
	}
	for code, want := range cases {
		require.Equal(t, want, HTTPStatus(code), code.String())
	}
}
