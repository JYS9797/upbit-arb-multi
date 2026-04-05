package exchange

import (
	"crypto/sha512"
	"encoding/hex"
	"fmt"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// BuildJWT builds Authorization header value "Bearer <jwt>"
// If query is non-empty, include query_hash fields (SHA512).
func BuildJWT(accessKey, secretKey, query string) (string, error) {
	claims := jwt.MapClaims{
		"access_key": accessKey,
		"nonce":      uuid.NewString(),
	}
	if query != "" {
		h := sha512.Sum512([]byte(query))
		claims["query_hash"] = hex.EncodeToString(h[:])
		claims["query_hash_alg"] = "SHA512"
	}

	t := jwt.NewWithClaims(jwt.SigningMethodHS512, claims)
	s, err := t.SignedString([]byte(secretKey))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Bearer %s", s), nil
}
