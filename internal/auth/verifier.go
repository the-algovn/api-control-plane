// Package auth verifies Zitadel-issued Bearer JWTs and evaluates route rules.
package auth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

const rolesClaim = "urn:zitadel:iam:org:project:roles"

var ErrNotReady = errors.New("jwks not loaded yet")

type Identity struct {
	Sub           string
	Roles         map[string]struct{}
	Authenticated bool
	ExpiresAt     time.Time // token exp; zero when unauthenticated
}

type Verifier struct {
	issuer    string
	audiences []string // accepted aud values; empty disables the check
	kf        atomic.Pointer[jwt.Keyfunc]
}

// NewVerifier starts a background loop that fetches the JWKS with backoff.
// keyfunc.NewDefaultOverrideCtx auto-refreshes (hourly + rate-limited on
// unknown kid), so Zitadel key rotation needs no restarts. The first-fetch
// error is not swallowed (NoErrorReturnFirstHTTPReq: false), so an
// unreachable JWKS endpoint keeps the verifier not-ready and retrying
// instead of reporting ready with an empty key set.
func NewVerifier(ctx context.Context, issuer, jwksURL string, audiences []string, logger *slog.Logger) *Verifier {
	v := &Verifier{issuer: issuer, audiences: audiences}
	go func() {
		backoff := time.Second
		noSwallow := false
		for {
			// Per-attempt child context: jwkset spawns its hourly-refresh
			// goroutine (bound to the ctx we pass) BEFORE the synchronous
			// first fetch, so a failed attempt would otherwise leak a
			// goroutine that keeps polling the JWKS URL until process exit.
			attemptCtx, cancel := context.WithCancel(ctx)
			kf, err := keyfunc.NewDefaultOverrideCtx(attemptCtx, []string{jwksURL}, keyfunc.Override{
				NoErrorReturnFirstHTTPReq: &noSwallow,
			})
			if err == nil {
				// Success: attemptCtx must stay alive for keyfunc's refresh
				// goroutine; it is cancelled transitively via the parent ctx.
				_ = cancel // do NOT cancel; parent ctx owns shutdown
				var f jwt.Keyfunc = kf.Keyfunc
				v.kf.Store(&f)
				logger.Info("jwks loaded", "url", jwksURL)
				return
			}
			cancel() // reap the orphaned refresh goroutine from this failed attempt
			logger.Warn("jwks fetch failed; retrying", "url", jwksURL, "backoff", backoff, "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
		}
	}()
	return v
}

func (v *Verifier) Ready() bool { return v.kf.Load() != nil }

func (v *Verifier) Verify(token string) (Identity, error) {
	kf := v.kf.Load()
	if kf == nil {
		return Identity{}, ErrNotReady
	}
	claims := jwt.MapClaims{}
	_, err := jwt.ParseWithClaims(token, claims, *kf,
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithIssuer(v.issuer),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(30*time.Second),
	)
	if err != nil {
		return Identity{}, fmt.Errorf("invalid token: %w", err)
	}
	if len(v.audiences) > 0 {
		aud, _ := claims.GetAudience()
		if !audienceAllowed(v.audiences, aud) {
			return Identity{}, fmt.Errorf("invalid token: audience %v not accepted", []string(aud))
		}
	}
	sub, err := claims.GetSubject()
	if err != nil || sub == "" {
		return Identity{}, fmt.Errorf("invalid token: missing subject")
	}
	id := Identity{Sub: sub, Roles: map[string]struct{}{}, Authenticated: true}
	if exp, e := claims.GetExpirationTime(); e == nil && exp != nil {
		id.ExpiresAt = exp.Time
	}
	if raw, ok := claims[rolesClaim].(map[string]any); ok {
		for role := range raw {
			id.Roles[role] = struct{}{}
		}
	}
	return id, nil
}

// audienceAllowed reports whether the token's aud claim contains at least one
// of the accepted audiences.
func audienceAllowed(allowed []string, aud jwt.ClaimStrings) bool {
	for _, a := range aud {
		if slices.Contains(allowed, a) {
			return true
		}
	}
	return false
}
