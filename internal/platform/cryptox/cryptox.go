package cryptox

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
)

type FieldCipher struct {
	aead cipher.AEAD
}

func NewFieldCipherFromBase64(encodedKey string) (*FieldCipher, error) {
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encodedKey))
	if err != nil {
		return nil, fmt.Errorf("decode encryption key: %w", err)
	}
	switch len(key) {
	case 16, 24, 32:
	default:
		return nil, fmt.Errorf("invalid encryption key length: %d", len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create gcm: %w", err)
	}
	return &FieldCipher{aead: aead}, nil
}

func (c *FieldCipher) Encrypt(plaintext []byte) ([]byte, []byte, error) {
	if c == nil {
		return nil, nil, fmt.Errorf("field cipher is nil")
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, fmt.Errorf("read nonce: %w", err)
	}
	return c.aead.Seal(nil, nonce, plaintext, nil), nonce, nil
}

func (c *FieldCipher) Decrypt(ciphertext, nonce []byte) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("field cipher is nil")
	}
	plaintext, err := c.aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt ciphertext: %w", err)
	}
	return plaintext, nil
}
