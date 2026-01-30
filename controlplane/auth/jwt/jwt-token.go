package auth

import (
	"time"
	"fmt"
	
	"github.com/00pauln00/niova-lookout/pkg/xlog"
	"github.com/golang-jwt/jwt/v5"
)

type Token struct {
	secret []byte
	userID string
	ttl    time.Duration
}

func (tc *Token) CreateToken(customClaims map[string]any) (string, error) {

	xlog.Trace("CreateToken: enter")
	defer xlog.Trace("CreateToken: exit")

	now := time.Now()

	claims := jwt.MapClaims{}

	for k, v := range customClaims {
		claims[k] = v
	}
	// Enforce registered claims
	claims["iss"] = tc.userID
	claims["exp"] = jwt.NewNumericDate(now.Add(tc.ttl))

	xlog.Debug("Provided token claims: ", claims)
	
	token := jwt.NewWithClaims(jwt.SigningMethodHS512, claims)

	tokenString, err := token.SignedString(tc.secret)
	if err != nil {
		xlog.Error( "Failed to create the token string: ", err)
		return "", err
	}

	xlog.Debug("Token size in (bytes):", len(tokenString))

	return tokenString, nil
}

func CreateAuthToken (secret, userid string, claims map[string]any, ttl time.Duration)( string, error) {

	xlog.Trace("CreateAuthToken: enter")
	defer xlog.Trace("CreateAuthToken: exit")

	xlog.Debugf("Creating auth token: issuer=%s ttl=%s", userid, ttl)
	tc := &Token{
		secret: []byte(secret),
		userID: userid,
		ttl:    ttl,
	}
	
	authtoken, err := tc.CreateToken(claims)
	if err != nil {
		xlog.Error("Error creating Auth Token: ",err)
		return "", err
	}
	xlog.Info("JWT token created successfully: ", authtoken)
	return authtoken, nil
}

func VerifyAuthToken( tokenString, secret string) (jwt.MapClaims, error) {

	xlog.Trace("VerifyAuthToken: enter")
	defer xlog.Trace("VerifyAuthToken: exit")

	xlog.Debug("Verifying auth token with token: ", tokenString, " secret: ", secret)
	
	claims := jwt.MapClaims{}

	token, err := jwt.ParseWithClaims(tokenString, claims,
		func(token *jwt.Token) (any, error) {
			// Enforce signing algorithm
			if token.Method != jwt.SigningMethodHS512 {
				xlog.Error("Unexpected signing method: ", token.Method)
				return nil, fmt.Errorf("unexpected signing method: %v",
					token.Header["alg"],
				)
			}
			return []byte(secret), nil
			},
	)
	
    if err != nil {
        xlog.Error("jwt verify: parse failed:", err)
        return nil, err
    }

    if !token.Valid {
        err := fmt.Errorf("invalid token")
        xlog.Error("jwt verify: token invalid")
        return nil, err
    }

    xlog.Info("jwt verify: token verified successfully", claims)
    return claims, nil
}
