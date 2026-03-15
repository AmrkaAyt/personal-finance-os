package rbac

import (
	"net/http"

	"personal-finance-os/internal/platform/httpx"
	"personal-finance-os/internal/platform/jwtx"
)

func HasAnyRole(claims *jwtx.Claims, roles ...string) bool {
	if claims == nil {
		return false
	}
	allowed := make(map[string]struct{}, len(roles))
	for _, role := range roles {
		allowed[role] = struct{}{}
	}
	for _, role := range claims.Roles {
		if _, ok := allowed[role]; ok {
			return true
		}
	}
	return false
}

func RequireRoles(roles ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims, ok := jwtx.ClaimsFromContext(r.Context())
			if !ok || !HasAnyRole(claims, roles...) {
				httpx.JSON(w, http.StatusForbidden, map[string]string{"error": "forbidden"})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
