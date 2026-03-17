package jwtx

import (
	"context"
	"net/http"
	"strings"

	"personal-finance-os/internal/platform/httpx"
)

type contextKey string

const claimsKey contextKey = "jwt_claims"

func Middleware(manager *Manager, required bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authorization := strings.TrimSpace(r.Header.Get("Authorization"))
			if authorization == "" {
				if required {
					httpx.JSON(w, http.StatusUnauthorized, map[string]string{"error": "missing authorization header"})
					return
				}
				next.ServeHTTP(w, r)
				return
			}

			parts := strings.SplitN(authorization, " ", 2)
			if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
				httpx.JSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid authorization header"})
				return
			}

			claims, err := manager.Parse(parts[1])
			if err != nil {
				httpx.JSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid token"})
				return
			}
			if claims.Type != "access" {
				httpx.JSON(w, http.StatusUnauthorized, map[string]string{"error": "token is not an access token"})
				return
			}

			ctx := context.WithValue(r.Context(), claimsKey, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func ClaimsFromContext(ctx context.Context) (*Claims, bool) {
	claims, ok := ctx.Value(claimsKey).(*Claims)
	return claims, ok
}
