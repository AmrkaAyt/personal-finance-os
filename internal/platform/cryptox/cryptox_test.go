package cryptox

import (
	"crypto/rand"
	"encoding/base64"
	"testing"
)

func TestFieldCipherRoundTrip(t *testing.T) {
	t.Parallel()

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("rand.Read returned error: %v", err)
	}

	cipher, err := NewFieldCipherFromBase64(base64.StdEncoding.EncodeToString(key))
	if err != nil {
		t.Fatalf("NewFieldCipherFromBase64 returned error: %v", err)
	}

	plaintext := []byte("sensitive-bank-statement")
	ciphertext, nonce, err := cipher.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt returned error: %v", err)
	}
	if string(ciphertext) == string(plaintext) {
		t.Fatal("ciphertext should not match plaintext")
	}

	decrypted, err := cipher.Decrypt(ciphertext, nonce)
	if err != nil {
		t.Fatalf("Decrypt returned error: %v", err)
	}
	if string(decrypted) != string(plaintext) {
		t.Fatalf("unexpected decrypted payload: %s", decrypted)
	}
}

func TestKeyringEncryptDecryptWithLegacyKey(t *testing.T) {
	t.Parallel()

	currentKey := make([]byte, 32)
	if _, err := rand.Read(currentKey); err != nil {
		t.Fatalf("rand.Read currentKey: %v", err)
	}
	legacyKey := make([]byte, 32)
	if _, err := rand.Read(legacyKey); err != nil {
		t.Fatalf("rand.Read legacyKey: %v", err)
	}

	keyring, err := NewKeyring(
		"v2",
		base64.StdEncoding.EncodeToString(currentKey),
		"v1="+base64.StdEncoding.EncodeToString(legacyKey),
	)
	if err != nil {
		t.Fatalf("NewKeyring returned error: %v", err)
	}

	ciphertext, nonce, keyID, err := keyring.Encrypt([]byte("secret"))
	if err != nil {
		t.Fatalf("Encrypt returned error: %v", err)
	}
	if keyID != "v2" {
		t.Fatalf("unexpected key id: %s", keyID)
	}

	decrypted, err := keyring.Decrypt(ciphertext, nonce, keyID)
	if err != nil {
		t.Fatalf("Decrypt current returned error: %v", err)
	}
	if string(decrypted) != "secret" {
		t.Fatalf("unexpected decrypted payload: %s", decrypted)
	}

	legacyCipher, err := NewFieldCipherFromBase64(base64.StdEncoding.EncodeToString(legacyKey))
	if err != nil {
		t.Fatalf("NewFieldCipherFromBase64 legacy returned error: %v", err)
	}
	legacyCiphertext, legacyNonce, err := legacyCipher.Encrypt([]byte("old-secret"))
	if err != nil {
		t.Fatalf("legacy Encrypt returned error: %v", err)
	}

	legacyDecrypted, err := keyring.Decrypt(legacyCiphertext, legacyNonce, "v1")
	if err != nil {
		t.Fatalf("legacy Decrypt returned error: %v", err)
	}
	if string(legacyDecrypted) != "old-secret" {
		t.Fatalf("unexpected legacy decrypted payload: %s", legacyDecrypted)
	}
}
