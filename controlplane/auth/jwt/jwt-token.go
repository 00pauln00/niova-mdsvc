package auth

import (
	"time"
	"log"
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

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

func (tc *TokenCreator) CreateToken(customClaims map[string]any) (string, error) {
	now := time.Now()

	claims := jwt.MapClaims{}

	for k, v := range customClaims {
		claims[k] = v
	}
	// Enforce registered claims
	claims["iss"] = tc.userID
	claims["exp"] = jwt.NewNumericDate(now.Add(tc.ttl))
	
	token := jwt.NewWithClaims(jwt.SigningMethodHS512, claims)

	tokenString, err := token.SignedString(tc.secret)
	if err != nil {
		log.Fatal( "Failed to create the token string: ", err)
		return "", err
	}

	log.Println("JWT Token:", tokenString)
	log.Println("Token size (bytes):", len(tokenString))

	return tokenString, nil
}

func CreateAuthToken (secret, userid string, claims map[string]any, ttl time.Duration)( string, error) {
	tc := NewTokenCreator(secret, userid, ttl)
	authtoken, err := tc.CreateToken(claims)
	if err != nil {
		log.Fatal("Error creating Auth Token: ",err)
		return "", err
	}
	return authtoken, nil
}

func VerifyAuthToken( tokenString, secret string) (jwt.MapClaims, error) {
	
	claims := jwt.MapClaims{}

	token, err := jwt.ParseWithClaims(tokenString, claims,
		func(token *jwt.Token) (any, error) {
			// Enforce signing algorithm
			if token.Method != jwt.SigningMethodHS512 {
				return nil, fmt.Errorf("unexpected signing method: %v",
					token.Header["alg"],
				)
			}
			return []byte(secret), nil
			},
	)

    if err != nil {
        log.Println("jwt verify: parse failed:", err)
        return nil, err
    }

    if !token.Valid {
        err := fmt.Errorf("invalid token")
        log.Println("jwt verify: token invalid")
        return nil, err
    }

    log.Println("jwt verify: token verified successfully", claims)
    return claims, nil
}
