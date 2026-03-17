package userctx

import (
	"errors"
	"net/http"
	"strings"
)

const HeaderUserID = "X-User-ID"

var ErrUnauthorized = errors.New("unauthorized")

func UserID(r *http.Request) string {
	return strings.TrimSpace(r.Header.Get(HeaderUserID))
}

func RequireAuthenticatedUserID(r *http.Request) (string, error) {
	if strings.TrimSpace(r.URL.Query().Get("user_id")) != "" {
		return "", ErrUnauthorized
	}
	userID := UserID(r)
	if userID == "" {
		return "", ErrUnauthorized
	}
	return userID, nil
}

func SetAuthenticatedUserID(r *http.Request, userID string) {
	r.Header.Set(HeaderUserID, strings.TrimSpace(userID))
}

func StripUserIDQuery(r *http.Request) {
	query := r.URL.Query()
	query.Del("user_id")
	r.URL.RawQuery = query.Encode()
}
