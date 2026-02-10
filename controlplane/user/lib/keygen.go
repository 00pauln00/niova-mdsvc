package userlib

import (
	"crypto/rand"
	"encoding/base64"
	"io"
)

// SecretKeyLengh defines lenght of secret key in bytes (128 bits)
const SecretKeyLength = 16

// GenerateSecretKey generates a cryptographically secure random secret key
// using systems CSPRNG
// returns:
//   - Base64-encoded secret key string
//   - Error if any while generation
func GenerateSecretKey() (string, error) {
	key := make([]byte, SecretKeyLength)

	// fill with cryptographically random bytes
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return "", err
	}

	// encode to base64 for safe transmission
	encodedKey := base64.RawStdEncoding.EncodeToString(key)

	return encodedKey, nil
}

// ValidateKey checks if secret key is valid
func ValidateKey(secretKey string) bool {
	// cannot be empty
	if secretKey == "" {
		return false
	}

	// decode to verify valid base64 key
	decoded, err := base64.RawStdEncoding.DecodeString(secretKey)
	if err != nil {
		return false
	}
	// verify decoded string length
	return len(decoded) == SecretKeyLength
}
