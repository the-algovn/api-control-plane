package auth

import (
	"errors"
	"strings"
)

type AuthError struct {
	Status  int
	Code    string
	Message string
}

func (e *AuthError) Error() string { return e.Message }

var (
	errUnauthorized = &AuthError{Status: 401, Code: "unauthenticated", Message: "missing or invalid bearer token"}
	errForbidden    = &AuthError{Status: 403, Code: "permission_denied", Message: "insufficient role"}
	errNotReady     = &AuthError{Status: 503, Code: "unavailable", Message: "auth keys not loaded yet"}
)

// Authorize enforces a route/channel rule against the raw Authorization
// header. Rules are validated at config-load time (config.ValidRule); an
// unknown rule here fails closed as 403.
func Authorize(v *Verifier, rule, authorization string) (Identity, *AuthError) {
	token, present, malformed := parseBearer(authorization)

	// Anonymous routes never reject on credentials: a valid token yields an
	// identity, anything else (absent, malformed, invalid, or keys-not-ready)
	// falls back to unauthenticated access.
	if rule == "anonymous" {
		if present {
			if id, err := v.Verify(token); err == nil {
				return id, nil
			}
		}
		return Identity{}, nil
	}

	if malformed {
		return Identity{}, errUnauthorized // header present but not a valid Bearer token
	}
	if !present {
		return Identity{}, errUnauthorized
	}

	id, err := v.Verify(token)
	switch {
	case errors.Is(err, ErrNotReady):
		return Identity{}, errNotReady
	case err != nil:
		return Identity{}, errUnauthorized
	}

	switch {
	case rule == "authenticated":
		return id, nil
	case strings.HasPrefix(rule, "role:"):
		if _, ok := id.Roles[strings.TrimPrefix(rule, "role:")]; ok {
			return id, nil
		}
		return Identity{}, errForbidden
	default:
		return Identity{}, errForbidden
	}
}

// parseBearer extracts the token from an Authorization header. The auth scheme
// is matched case-insensitively per RFC 6750. It returns present when the
// header is a well-formed "Bearer <token>" with a non-empty token, and
// malformed when a non-empty header is not a valid bearer token (wrong scheme,
// missing space, or empty token). An empty header is neither (no credentials).
func parseBearer(authorization string) (token string, present, malformed bool) {
	if authorization == "" {
		return "", false, false
	}
	const scheme = "bearer "
	if len(authorization) < len(scheme) || !strings.EqualFold(authorization[:len(scheme)], scheme) {
		return "", false, true
	}
	token = strings.TrimSpace(authorization[len(scheme):])
	if token == "" {
		return "", false, true
	}
	return token, true, false
}
