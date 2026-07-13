package auth

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"

	"github.com/the-algovn/api-control-plane/internal/auth/authtest"
)

const issuer = "https://id.algovn.com"

func newVerifier(t *testing.T) (*Verifier, *authtest.JWKS) {
	t.Helper()
	jwks := authtest.NewJWKS(t)
	srv := jwks.Server(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	v := NewVerifier(ctx, issuer, srv.URL, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	require.Eventually(t, v.Ready, 5*time.Second, 20*time.Millisecond)
	return v, jwks
}

func validClaims() jwt.MapClaims {
	return jwt.MapClaims{
		"iss": issuer,
		"sub": "user-123",
		"exp": time.Now().Add(time.Hour).Unix(),
		"urn:zitadel:iam:org:project:roles": map[string]any{
			"admin": map[string]any{"318": "algovn.com"},
		},
	}
}

func TestVerify(t *testing.T) {
	v, jwks := newVerifier(t)

	id, err := v.Verify(jwks.Sign(t, validClaims()))
	require.NoError(t, err)
	require.Equal(t, "user-123", id.Sub)
	require.True(t, id.Authenticated)
	require.Contains(t, id.Roles, "admin")

	// wrong issuer
	c := validClaims()
	c["iss"] = "https://evil.example"
	_, err = v.Verify(jwks.Sign(t, c))
	require.Error(t, err)

	// expired
	c = validClaims()
	c["exp"] = time.Now().Add(-2 * time.Hour).Unix()
	_, err = v.Verify(jwks.Sign(t, c))
	require.Error(t, err)

	// garbage
	_, err = v.Verify("not.a.jwt")
	require.Error(t, err)
}

func TestAuthorize(t *testing.T) {
	v, jwks := newVerifier(t)
	valid := "Bearer " + jwks.Sign(t, validClaims())
	noRoles := validClaims()
	delete(noRoles, "urn:zitadel:iam:org:project:roles")
	member := "Bearer " + jwks.Sign(t, noRoles)

	cases := []struct {
		name, rule, authz string
		wantStatus        int // 0 = allowed
		wantSub           string
	}{
		{"anon no token", "anonymous", "", 0, ""},
		{"anon valid token", "anonymous", valid, 0, "user-123"},
		{"anon bad token", "anonymous", "Bearer garbage", 401, ""},
		{"authed no token", "authenticated", "", 401, ""},
		{"authed valid", "authenticated", valid, 0, "user-123"},
		{"role match", "role:admin", valid, 0, "user-123"},
		{"role missing", "role:admin", member, 403, ""},
		{"role no token", "role:admin", "", 401, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, aerr := Authorize(v, tc.rule, tc.authz)
			if tc.wantStatus == 0 {
				require.Nil(t, aerr)
				require.Equal(t, tc.wantSub, id.Sub)
			} else {
				require.NotNil(t, aerr)
				require.Equal(t, tc.wantStatus, aerr.Status)
			}
		})
	}
}

func TestAuthorize_NotReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// point at a dead JWKS endpoint: verifier never becomes ready
	v := NewVerifier(ctx, issuer, "http://127.0.0.1:1/jwks", slog.New(slog.NewTextHandler(os.Stderr, nil)))

	_, aerr := Authorize(v, "authenticated", "Bearer whatever")
	require.NotNil(t, aerr)
	require.Equal(t, 503, aerr.Status)

	// anonymous without a token still works while JWKS is down
	id, aerr := Authorize(v, "anonymous", "")
	require.Nil(t, aerr)
	require.False(t, id.Authenticated)
}
