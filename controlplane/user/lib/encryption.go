package userlib

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	log "github.com/00pauln00/niova-lookout/pkg/xlog"
)

const (
	expectedKeyLength = 32 // AES-256
)

var (
	once     sync.Once
	instance *encrypter
	initErr  error
)

// encrypter is the internal implementation (private)
type encrypter struct {
	gcm cipher.AEAD
}

// Encrypt encrypts plaintext using AES-256-GCM
func (e *encrypter) Encrypt(plaintext string) (string, error) {
	if plaintext == "" {
		return "", errors.New("plaintext cannot be empty")
	}

	nonce := make([]byte, e.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("nonce generation failed: %w", err)
	}

	ciphertext := e.gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.RawStdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt decrypts data previously encrypted by Encrypt
func (e *encrypter) Decrypt(encrypted string) (string, error) {
	if encrypted == "" {
		return "", errors.New("encrypted text cannot be empty")
	}

	data, err := base64.RawStdEncoding.DecodeString(encrypted)
	if err != nil {
		return "", fmt.Errorf("base64 decoding failed: %w", err)
	}

	nonceSize := e.gcm.NonceSize()
	if len(data) < nonceSize {
		return "", fmt.Errorf("ciphertext too short: need at least %d bytes, got %d", nonceSize, len(data))
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plaintext, err := e.gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("decryption failed (wrong key or corrupted data): %w", err)
	}

	return string(plaintext), nil
}

// Initialize loads the encryption key exactly once
// You should call this early in your application (main, tests setup, etc.)
func Initialize() error {
	once.Do(func() {
		logLevel := "trace"
		log.InitXlog("", &logLevel)

		keySrc := os.Getenv("USER_ENCRYPTION_KEY")
		if keySrc == "" {
			initErr = errors.New("USER_ENCRYPTION_KEY environment variable is required")
			log.Error("FATAL: USER_ENCRYPTION_KEY not set")
			return
		}

		key, err := base64.RawStdEncoding.DecodeString(keySrc)
		if err != nil {
			initErr = fmt.Errorf("invalid base64 key: %w", err)
			log.Error("FATAL: Invalid encryption key format")
			return
		}

		if len(key) != expectedKeyLength {
			initErr = fmt.Errorf("encryption key must be %d bytes, got %d", expectedKeyLength, len(key))
			log.Error("FATAL: Wrong encryption key length")
			return
		}

		block, err := aes.NewCipher(key)
		if err != nil {
			initErr = fmt.Errorf("failed to create AES cipher: %w", err)
			return
		}

		gcm, err := cipher.NewGCM(block)
		if err != nil {
			initErr = fmt.Errorf("failed to create GCM: %w", err)
			return
		}

		instance = &encrypter{gcm: gcm}
		log.Info("Cluster encryption initialized successfully")
	})

	return initErr
}

// MustInitialize panics if initialization fails
// Useful for main() or critical services
func MustInitialize() {
	if err := Initialize(); err != nil {
		log.Error("Encryption initialization failed", "error", err)
		panic(fmt.Sprintf("encryption is mandatory: %v", err))
	}
}

// EncryptSecretKey encrypts using the global encrypter
func EncryptSecretKey(plaintext string) (string, error) {
	if err := ensureInitialized(); err != nil {
		return "", err
	}
	return instance.Encrypt(plaintext)
}

// DecryptSecretKey decrypts using the global encrypter
func DecryptSecretKey(encrypted string) (string, error) {
	if err := ensureInitialized(); err != nil {
		return "", err
	}
	return instance.Decrypt(encrypted)
}

// GetKeyLength returns the required key length in bytes
func GetKeyLength() int {
	return expectedKeyLength
}

// ensureInitialized checks if the encrypter is ready
func ensureInitialized() error {
	if initErr != nil {
		return fmt.Errorf("encryption initialization failed earlier: %w", initErr)
	}
	if instance == nil {
		return errors.New("encryption not initialized - call userlib.Initialize() first")
	}
	return nil
}
