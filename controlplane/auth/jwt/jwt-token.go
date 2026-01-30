package auth

import (
	"time"
	"fmt"
	
	"github.com/00pauln00/niova-lookout/pkg/xlog"
	"github.com/golang-jwt/jwt/v5"
)

type Token struct {
	Secret []byte
	UserID string
	TTL    time.Duration
}

func (tc *Token) CreateToken(customClaims map[string]any) (string, error) {
	xlog.Trace("CreateToken: enter")
	defer xlog.Trace("CreateToken: exit")
	xlog.Debugf("Creating token: issuer=%s ttl=%s", tc.UserID, tc.TTL)
	now := time.Now()
	claims := jwt.MapClaims{}
	for k, v := range customClaims {
		claims[k] = v
	}
	// Enforce registered claims
	claims["iss"] = tc.UserID
	claims["exp"] = jwt.NewNumericDate(now.Add(tc.TTL))
	xlog.Debug("Provided token claims: ", claims)
	token := jwt.NewWithClaims(jwt.SigningMethodHS512, claims)
	tokenString, err := token.SignedString(tc.Secret)
	if err != nil {
		xlog.Error( "Failed to create the token string: ", err)
		return "", err
	}
	xlog.Debug("Token size in (bytes):", len(tokenString))
	xlog.Info("JWT token created successfully: ", tokenString)
	return tokenString, nil
}

func (tc *Token) VerifyToken( tokenString string) (jwt.MapClaims, error) {
	xlog.Trace("VerifyAuthToken: enter")
	defer xlog.Trace("VerifyAuthToken: exit")
	xlog.Debug("Verifying the Token")
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
			return []byte(tc.Secret), nil
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
