package security

import (
	"fmt"
	"os"
	"strings"
)

const MinSecretLength = 16

type Secret string

func (s Secret) String() string {
	return "[REDACTED]"
}

func (s Secret) Reveal() string {
	return string(s)
}

func (s Secret) Empty() bool {
	return s == ""
}

func LoadSecret(env, file string) (Secret, error) {
	if (env == "") == (file == "") {
		return "", ErrSecretSourceAmbiguous
	}

	raw, err := readSecret(env, file)
	if err != nil {
		return "", err
	}

	secret := strings.TrimSpace(raw)
	if secret == "" {
		return "", ErrSecretEmpty
	}

	if len(secret) < MinSecretLength {
		return "", fmt.Errorf("%w: need at least %d characters", ErrSecretTooShort, MinSecretLength)
	}

	return Secret(secret), nil
}

func readSecret(env, file string) (string, error) {
	if env != "" {
		return os.Getenv(env), nil
	}

	data, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("read secret file %q: %w", file, err)
	}

	return string(data), nil
}
