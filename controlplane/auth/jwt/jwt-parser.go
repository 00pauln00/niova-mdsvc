package auth

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type Claims struct {
	Vdevuuid string
	jwt.RegisteredClaims
}

type TokenCreator struct {
	secret []byte
	userID string
	ttl    time.Duration
}

func NewTokenCreator(secret, userid string, ttl time.Duration) *TokenCreator {
	return &TokenCreator{
		secret: []byte(secret),
		userID: userid,
		ttl:    ttl,
	}
}

func (tc *TokenCreator) CreateToken(vdevID string) (string, error) {
	now := time.Now()

	claims := Claims{
		Vdevuuid: vdevID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    tc.userID,
			ExpiresAt: jwt.NewNumericDate(now.Add(tc.ttl)),
		},
	}

	// 🔍 Print claims before signing
	fmt.Printf("Claims:\n  Vdevuuid: %s\n Issuer: %s\n  ExpiresAt: %v\n",
		claims.Vdevuuid,
		claims.Issuer,
		claims.ExpiresAt.Time,
	)

	token := jwt.NewWithClaims(jwt.SigningMethodHS512, claims)

	tokenString, err := token.SignedString(tc.secret)
	if err != nil {
		return "", err
	}

	// 🔐 Print token string and size
	fmt.Println("JWT Token:", tokenString)
	fmt.Println("Token size (bytes):", len(tokenString))

	return tokenString, nil
}

func CreateAuthToken (secret, userid, vdevID string, ttl time.Duration)( string, error) {
	tc := NewTokenCreator(secret, userid, ttl)
	authtoken, err := tc.CreateToken(vdevID)
	if err != nil {
		fmt.Println("Error creating Auth Token: ",err)
	}
	return authtoken, nil
}

