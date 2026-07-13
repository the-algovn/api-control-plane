package transcode

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/dynamicpb"
)

var marshaler = protojson.MarshalOptions{} // proto3 JSON defaults

// Invoke calls a unary method with a JSON request body and returns the JSON
// response. Errors are gRPC status errors (map with HTTPStatus), except
// ErrMethodNotFound which the caller maps to 404.
func (b *Backend) Invoke(ctx context.Context, svcMethod string, jsonBody []byte, md metadata.MD, deadline time.Duration) ([]byte, error) {
	desc, err := b.Method(svcMethod)
	if err != nil {
		return nil, err
	}
	in := dynamicpb.NewMessage(desc.Input())
	if len(jsonBody) > 0 {
		if err := protojson.Unmarshal(jsonBody, in); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "request body: %v", err)
		}
	}
	out := dynamicpb.NewMessage(desc.Output())

	ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(ctx, md), deadline)
	defer cancel()
	if err := b.conn.Invoke(ctx, "/"+svcMethod, in, out); err != nil {
		return nil, err // already a status error
	}
	respJSON, err := marshaler.Marshal(out)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal response: %v", err)
	}
	return respJSON, nil
}

// HTTPStatus maps gRPC codes to HTTP statuses (spec §6).
func HTTPStatus(c codes.Code) int {
	switch c {
	case codes.OK:
		return 200
	case codes.InvalidArgument:
		return 400
	case codes.Unauthenticated:
		return 401
	case codes.PermissionDenied:
		return 403
	case codes.NotFound:
		return 404
	case codes.Unavailable:
		return 502
	case codes.DeadlineExceeded:
		return 504
	default:
		return 500
	}
}
