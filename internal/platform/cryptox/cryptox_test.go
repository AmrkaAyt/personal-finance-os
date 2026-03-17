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
