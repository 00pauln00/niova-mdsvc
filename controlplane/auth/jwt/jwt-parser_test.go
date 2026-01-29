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
	customclaims := map[string]any{
		"vdevID": vdevID,
	}

	tokenString, err := CreateAuthToken(secret, userID, customclaims, ttl)
	if err != nil {
		t.Fatalf("CreateToken failed: %v", err)
	}

	if tokenString == "" {
		t.Fatal("expected token string, got empty")
	}

	claims := jwt.MapClaims{}

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
	// ---- CLAIM CHECKS ----

	// vdevID (custom claim)
	gotVdevID, ok := claims["vdevID"].(string)
	if !ok {
		t.Fatal("vdevID claim missing or not a string")
	}
	if gotVdevID != vdevID {
		t.Fatalf("unexpected vdevID: got=%s want=%s", gotVdevID, vdevID)
	}

	// issuer (iss)
	iss, ok := claims["iss"].(string)
	if !ok {
		t.Fatal("iss claim missing or not a string")
	}
	if iss != userID {
		t.Fatalf("unexpected issuer: got=%s want=%s", iss, userID)
	}

	// expiration (exp)
	exp, ok := claims["exp"].(float64)
	if !ok {
		t.Fatal("exp claim missing or not numeric")
	}

	expTime := time.Unix(int64(exp), 0)
	now := time.Now()

	if expTime.Before(now) {
		t.Fatalf("token already expired: exp=%v now=%v", expTime, now)
	}

	// sanity check: expiration roughly matches ttl
	if expTime.After(now.Add(ttl + 5*time.Second)) {
		t.Fatalf("expiration too far in future: exp=%v", expTime)
	}
	t.Logf("PASS: JWT created and validated successfully | vdevID=%s issuer=%s expires_at=%v", gotVdevID, iss, expTime,)
}
