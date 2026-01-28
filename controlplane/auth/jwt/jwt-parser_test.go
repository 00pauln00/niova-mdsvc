package auth

import (
	"testing"
	"time"
	"fmt"
	"github.com/golang-jwt/jwt/v5"
)

func TestCreateToken(t *testing.T) {
	secret := "mysecretkey"
	userID := "239f524c-f796-11f0-8b18-f703cba9e01d"
	ttl := time.Minute

	vdevID := "17bb86da-f796-11f0-bc55-dbe415d16723"

	fmt.Println("Populated: vdevid= ", vdevID, " userid= ", userID, " claims.ExpiresAt= ", ttl)

	tokenString, err := CreateAuthToken(secret, userID, vdevID, ttl)
	if err != nil {
		t.Fatalf("CreateToken failed: %v", err)
	}

	if tokenString == "" {
		t.Fatal("expected token string, got empty")
	}

	claims := &Claims{}

	token, err := jwt.ParseWithClaims(
		tokenString,
		claims,
		func(token *jwt.Token) (interface{}, error) {
			// Algorithm check (important)
			if token.Method != jwt.SigningMethodHS512 {
				t.Fatalf("unexpected signing method: %v", token.Header["alg"])
			}
			return []byte(secret), nil
		},
	)

	if err != nil {
		t.Fatalf("failed to parse token: %v", err)
	}

	if !token.Valid {
		t.Fatal("token is invalid")
	}

	// Validate claims
	if claims.Vdevuuid != vdevID {
		t.Errorf("expected vdevuuid %q, got %q", vdevID, claims.Vdevuuid)
	}

	if claims.Issuer != userID {
		t.Errorf("expected issuer %q, got %q", userID, claims.Issuer)
	}

	if claims.ExpiresAt == nil {
		t.Fatal("expected ExpiresAt to be set")
	}

	if time.Until(claims.ExpiresAt.Time) <= 0 {
		t.Fatal("token already expired")
	}
	fmt.Println(" Parsed from token: vdevid= ", claims.Vdevuuid, " userid= ", claims.Issuer, " claims.ExpiresAt= ", claims.ExpiresAt)
}
