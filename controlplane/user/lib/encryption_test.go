package userlib

import (
	"encoding/base64"
	"testing"
)

/*
func TestMain(m *testing.M) {
	// Fixed 32-byte key for reproducible tests (do NOT use this in production!)
	fixedKey := []byte("0123456789abcdef0123456789abcdef")
	keyB64 := base64.RawStdEncoding.EncodeToString(fixedKey)

	// Set the env var for the duration of the tests
	os.Setenv("USER_ENCRYPTION_KEY", keyB64)

	if err := Initialize(); err != nil {
		fmt.Fprintf(os.Stderr, "Encryption initialization failed in TestMain: %v\n", err)
		os.Exit(1)
	}

	// Run all tests
	exitCode := m.Run()

	os.Exit(exitCode)
}
*/

func TestGetKeyLength(t *testing.T) {
	if got := GetKeyLength(); got != 32 {
		t.Errorf("GetKeyLength() = %d; want 32", got)
	}
}

func TestEncryptAndDecryptRoundTrip(t *testing.T) {
	if err := Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	testCases := []struct {
		name      string
		plaintext string
	}{
		{"simple", "hello world"},
		{"with special chars", " !@#$%^&*()_+-=[]{}|;':\",./<>?"},
		{"unicode", "こんにちは世界 🌍"},
		{"long string", "This is a longer plaintext to ensure the encryption/decryption works with bigger payloads. It contains multiple sentences and should be handled correctly by AES-GCM."},
		{"admin", "j4fMikA6jjC8Giuvz84gcQ"}, //"vyMpz1Wrw33oAGR9dYinK0/YQr3nSEt2/Mpz53MOvtnIt1LtyG6bf5KvSE1Q+6IM2cs"
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encrypted, err := EncryptSecretKey(tc.plaintext)
			if err != nil {
				t.Fatalf("Encrypt failed: %v", err)
			}
			if encrypted == "" {
				t.Error("encrypted string is empty")
			}

			decrypted, err := DecryptSecretKey(encrypted)
			if err != nil {
				t.Fatalf("Decrypt failed: %v", err)
			}
			if decrypted != tc.plaintext {
				t.Errorf("decrypted mismatch:\ngot  %q\nwant %q", decrypted, tc.plaintext)
			}
		})
	}

	// Verify that the same plaintext produces different ciphertexts (random nonce)
	t.Run("different nonces", func(t *testing.T) {
		plaintext := "same plaintext"
		enc1, err := EncryptSecretKey(plaintext)
		if err != nil {
			t.Fatal(err)
		}
		enc2, err := EncryptSecretKey(plaintext)
		if err != nil {
			t.Fatal(err)
		}
		if enc1 == enc2 {
			t.Error("two encryptions of the same plaintext produced identical ciphertext (nonce not random)")
		}
	})
}

func TestEncryptErrors(t *testing.T) {
	if err := Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	t.Run("empty plaintext", func(t *testing.T) {
		_, err := EncryptSecretKey("")
		if err == nil {
			t.Error("expected error for empty plaintext, got nil")
		}
	})
}

func TestDecryptErrors(t *testing.T) {
	if err := Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	t.Run("empty encrypted string", func(t *testing.T) {
		_, err := DecryptSecretKey("")
		if err == nil {
			t.Error("expected error for empty encrypted string, got nil")
		}
	})

	t.Run("invalid base64", func(t *testing.T) {
		_, err := DecryptSecretKey("!!!invalid-base64!!!")
		if err == nil {
			t.Error("expected base64 decode error, got nil")
		}
	})

	t.Run("ciphertext too short", func(t *testing.T) {
		// Create a valid base64 string that decodes to fewer than nonceSize bytes
		short := make([]byte, 11) // nonceSize is 12 for GCM
		shortEnc := base64.RawStdEncoding.EncodeToString(short)
		_, err := DecryptSecretKey(shortEnc)
		if err == nil {
			t.Error("expected 'ciphertext too short' error, got nil")
		}
	})

	t.Run("tampered ciphertext", func(t *testing.T) {
		plaintext := "tamper test"
		enc, err := EncryptSecretKey(plaintext)
		if err != nil {
			t.Fatal(err)
		}

		data, err := base64.RawStdEncoding.DecodeString(enc)
		if err != nil {
			t.Fatal(err)
		}

		if len(data) <= 12 {
			t.Fatal("unexpected short ciphertext")
		}

		// Flip a bit in the ciphertext portion (after nonce)
		data[12] ^= 0x01

		tampered := base64.RawStdEncoding.EncodeToString(data)

		_, err = DecryptSecretKey(tampered)
		if err == nil {
			t.Error("expected authentication error on tampered data, got nil")
		}
	})

	t.Run("tampered auth tag", func(t *testing.T) {
		plaintext := "auth tag test"
		enc, err := EncryptSecretKey(plaintext)
		if err != nil {
			t.Fatal(err)
		}

		data, err := base64.RawStdEncoding.DecodeString(enc)
		if err != nil {
			t.Fatal(err)
		}

		// Flip a bit in the last byte (part of the auth tag)
		data[len(data)-1] ^= 0x01

		tampered := base64.RawStdEncoding.EncodeToString(data)

		_, err = DecryptSecretKey(tampered)
		if err == nil {
			t.Error("expected authentication error on tampered tag, got nil")
		}
	})
}

func TestMultipleInitializeCalls(t *testing.T) {
	err1 := Initialize()
	if err1 != nil {
		t.Fatal(err1)
	}
	err2 := Initialize()
	if err2 != nil {
		t.Fatal(err2)
	}
	// No error on second call due to sync.Once
}
