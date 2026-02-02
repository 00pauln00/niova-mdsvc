package auth

import (
	"testing"
	"time"
	"fmt"
	"os"
	"encoding/base64"
	"strings"

	log "github.com/00pauln00/niova-lookout/pkg/xlog"
)

func TestMain(m *testing.M) {
    logLevel := "TRACE" // try TRACE / INFO / ERROR
    log.InitXlog("", &logLevel)
    os.Exit(m.Run())
}

func TestCreateToken(t *testing.T) {
	secret := "mysecretkey"
	userID := "239f524c-f796-11f0-8b18-f703cba9e01d"
	ttl := time.Minute
	vdevID := "17bb86da-f796-11f0-bc55-dbe415d16723"
	fmt.Println("Populated: vdevid= ", vdevID, " userid= ", userID, " claims.ExpiresAt= ", ttl)
	customclaims := map[string]any{
		"vdevID": vdevID,
		"userID": userID,
	}
	tc := &Token {
		Secret: []byte(secret),
		TTL: ttl,
	}
	tokenString, err := tc.CreateToken(customclaims)
	if err != nil {
		t.Fatalf("CreateToken failed: %v", err)
	}
	if tokenString == "" {
		t.Fatal("expected token string, got empty")
	}
	claims, err := tc.VerifyToken(tokenString)
	if err != nil {
		t.Fatalf("VerifyAuthToken failed: %v", err)
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
	if iss != TOKEN_ISSUER {
		t.Fatalf("unexpected issuer: got=%s want=%s", iss, TOKEN_ISSUER)
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

func TestVerifyAuthToken(t *testing.T) {
	secret := "mysecretkey"
	ttl := time.Minute
	vdevID := "17bb86da-f796-11f0-bc55-dbe415d16723"
	customClaims := map[string]any{
		"vdevID": vdevID,
	}
	tc := &Token {
			Secret: []byte(secret),
			TTL: ttl,
		}
	// Step 1: Create token
	tokenString, err := tc.CreateToken(customClaims)
	if err != nil {	
		t.Fatalf("CreateAuthToken failed: %v", err)
	}
	if tokenString == "" {
		t.Fatal("expected token string, got empty")
	}
	testsecret := "testsecret"
	tc2 := &Token {
			Secret: []byte(testsecret),
			TTL: ttl,
		  }
	// Step 2: Verify token
	claims, err := tc2.VerifyToken(tokenString)
	if err != nil {
		t.Fatalf("VerifyAuthToken failed: %v", err)
	}
	// Step 3: Validate custom claim
	gotVdevID, ok := claims["vdevID"].(string)
	if !ok {
		t.Fatal("vdevID claim missing or not string")
	}
	if gotVdevID != vdevID {
		t.Fatalf("unexpected vdevID: got=%s want=%s", gotVdevID, vdevID)
	}
	// Step 4: Validate issuer
	iss, ok := claims["iss"].(string)
	if !ok {
		t.Fatal("iss claim missing or not string")
	}
	if iss != TOKEN_ISSUER {
		t.Fatalf("unexpected issuer: got=%s want=%s", iss, TOKEN_ISSUER)
	}
	// Step 5: Validate expiration
	exp, ok := claims["exp"].(float64)
	if !ok {
		t.Fatal("exp claim missing or not numeric")
	}
	expTime := time.Unix(int64(exp), 0)
	if expTime.Before(time.Now()) {
		t.Fatalf("token expired unexpectedly: exp=%v", expTime)
	}
	// Final success message (only shown with -v)
	t.Logf("PASS: VerifyAuthToken succeeded | vdevID=%s issuer=%s expires_at=%v", gotVdevID, iss, expTime,)
}

func TestVerifyToken_TamperedToken(t *testing.T) {
	secret := "mysecretkey"
	ttl := time.Minute
	vdevID := "17bb86da-f796-11f0-bc55-dbe415d16723"
	customClaims := map[string]any{
		"vdevID": vdevID,
	}
	tc := &Token{
		Secret: []byte(secret),
		TTL:    ttl,
	}
	// Step 1: Create valid token
	tokenString, err := tc.CreateToken(customClaims)
	if err != nil {
		t.Fatalf("CreateToken failed: %v", err)
	}
	// Step 2: Tamper with token payload
	parts := strings.Split(tokenString, ".")
	if len(parts) != 3 {
		t.Fatal("invalid JWT format")
	}
	tamperedPayload := base64.RawURLEncoding.EncodeToString([]byte(`{"iss":"evil","vdevID":"hacked"}`),)
	tamperedToken := parts[0] + "." + tamperedPayload + "." + parts[2]
	// Step 3: Verify tampered token
	_, err = tc.VerifyToken(tamperedToken)
	if err == nil {
		t.Fatal("expected verification failure for tampered token")
	}
		t.Logf("PASS: tampered JWT correctly rejected | error=%v", err)
}

func TestVerifyToken_ExpiryAfterTTL(t *testing.T) {
	secret := "mysecretkey"
	issuer := "239f524c-f796-11f0-8b18-f703cba9e01d"
	vdevID := "17bb86da-f796-11f0-bc55-dbe415d16723"
	customClaims := map[string]any{
		"vdevID": vdevID,
		"userID": issuer,
	}
	// Very short TTL
	ttl := 1 * time.Second
	tc := &Token{
		Secret: []byte(secret),
		TTL:    ttl,
	}
	// Step 1: Create token
	tokenString, err := tc.CreateToken(customClaims)
	if err != nil {
		t.Fatalf("CreateToken failed: %v", err)
	}
	if tokenString == "" {
		t.Fatal("expected token string, got empty")
	}
	// Step 2: Sleep until token expires
	time.Sleep(2 * time.Second)
	// Step 3: Verify token (should fail)
	_, err = tc.VerifyToken(tokenString)
	if err == nil {
		t.Fatal("expected token verification to fail due to expiration")
	}
	t.Logf("PASS: JWT expired as expected | ttl=%s error=%v", ttl, err,)
}
