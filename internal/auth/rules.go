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
	token, hasToken := strings.CutPrefix(authorization, "Bearer ")
	if authorization != "" && !hasToken {
		return Identity{}, errUnauthorized // non-Bearer Authorization header
	}

	if rule == "anonymous" && !hasToken {
		return Identity{}, nil
	}
	if !hasToken {
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
	case rule == "anonymous", rule == "authenticated":
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
