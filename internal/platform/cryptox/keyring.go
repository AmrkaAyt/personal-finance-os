package cryptox

import (
	"fmt"
	"strings"
)

type Keyring struct {
	currentKeyID string
	current      *FieldCipher
	ciphers      map[string]*FieldCipher
}

func NewKeyring(currentKeyID, currentKeyB64, legacySpec string) (*Keyring, error) {
	keyID := strings.TrimSpace(currentKeyID)
	if keyID == "" {
		keyID = "default"
	}

	currentCipher, err := NewFieldCipherFromBase64(currentKeyB64)
	if err != nil {
		return nil, err
	}

	ciphers := map[string]*FieldCipher{
		keyID: currentCipher,
	}

	legacySpec = strings.TrimSpace(legacySpec)
	if legacySpec != "" {
		for _, item := range strings.Split(legacySpec, ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			legacyID, legacyKey, ok := strings.Cut(item, "=")
			if !ok {
				return nil, fmt.Errorf("invalid legacy key specification %q", item)
			}
			legacyID = strings.TrimSpace(legacyID)
			legacyKey = strings.TrimSpace(legacyKey)
			if legacyID == "" || legacyKey == "" {
				return nil, fmt.Errorf("invalid legacy key specification %q", item)
			}
			cipher, err := NewFieldCipherFromBase64(legacyKey)
			if err != nil {
				return nil, fmt.Errorf("legacy key %q: %w", legacyID, err)
			}
			ciphers[legacyID] = cipher
		}
	}

	return &Keyring{
		currentKeyID: keyID,
		current:      currentCipher,
		ciphers:      ciphers,
	}, nil
}

func (k *Keyring) CurrentKeyID() string {
	if k == nil {
		return ""
	}
	return k.currentKeyID
}

func (k *Keyring) Encrypt(plaintext []byte) ([]byte, []byte, string, error) {
	if k == nil || k.current == nil {
		return nil, nil, "", fmt.Errorf("keyring is nil")
	}
	ciphertext, nonce, err := k.current.Encrypt(plaintext)
	if err != nil {
		return nil, nil, "", err
	}
	return ciphertext, nonce, k.currentKeyID, nil
}

func (k *Keyring) Decrypt(ciphertext, nonce []byte, keyID string) ([]byte, error) {
	if k == nil {
		return nil, fmt.Errorf("keyring is nil")
	}
	resolved := strings.TrimSpace(keyID)
	if resolved == "" {
		resolved = k.currentKeyID
	}
	cipher, ok := k.ciphers[resolved]
	if !ok {
		return nil, fmt.Errorf("unknown encryption key id %q", resolved)
	}
	return cipher.Decrypt(ciphertext, nonce)
}
