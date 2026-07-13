// Package authtest provides a fake JWKS endpoint and token signer for tests.
package authtest

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

const KID = "test-key-1"

type JWKS struct {
	Key *rsa.PrivateKey
}

func NewJWKS(t *testing.T) *JWKS {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return &JWKS{Key: key}
}

// Server serves the public key as a JWK Set; closed via t.Cleanup by callers.
func (j *JWKS) Server(t *testing.T) *httptest.Server {
	t.Helper()
	pub := &j.Key.PublicKey
	n := base64.RawURLEncoding.EncodeToString(pub.N.Bytes())
	e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes())
	body := fmt.Sprintf(`{"keys":[{"kty":"RSA","use":"sig","alg":"RS256","kid":%q,"n":%q,"e":%q}]}`, KID, n, e)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// Sign issues an RS256 token with the given claims and the test kid.
func (j *JWKS) Sign(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = KID
	s, err := tok.SignedString(j.Key)
	require.NoError(t, err)
	return s
}
