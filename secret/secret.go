package secret

import (
	"bytes"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
)

type Secret struct {
	username, password []byte
}

func New(username, password []byte) *Secret {
	buf := make([]byte, 0, len(username)+len(password))
	buf = append(buf, username...)
	buf = append(buf, password...)

	return &Secret{
		username: buf[:len(username)],
		password: buf[len(username):],
	}
}

func (secret *Secret) Equal(other *Secret) bool {
	if secret == nil || other == nil {
		return secret == other
	}

	return subtle.ConstantTimeCompare(secret.password, other.password) == 1 &&
		subtle.ConstantTimeCompare(secret.username, other.username) == 1
}

func (secret *Secret) String() string {
	return fmt.Sprintf("%s:***", secret.username)
}

var encoding = base64.StdEncoding

func (secret *Secret) Encode() string {
	n := len(secret.username) + len(separator) + len(secret.password)

	str := &strings.Builder{}
	str.Grow(n)

	wr := base64.NewEncoder(encoding, str)

	wr.Write(secret.username)
	wr.Write(separator)
	wr.Write(secret.password)

	wr.Close()

	return str.String()
}

var (
	separator = []byte(":")

	errInvalidSecretFormat = errors.New("invalid secret format")
)

func (secret *Secret) UnmarshalText(data []byte) error {
	data = bytes.TrimSpace(data)

	username, password, ok := bytes.Cut(data, separator)
	if !ok || len(username) == 0 || len(password) == 0 {
		return errInvalidSecretFormat
	}

	secret.username = username
	secret.password = password

	return nil
}

const sizeLimit = 1024 * 1024

var errTooLarge = errors.New("secret file is too large")

func FromFile(filename string) (*Secret, error) {
	f, err := os.OpenFile(filename, os.O_RDONLY|syscall.O_NOFOLLOW, 0)

	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}
	defer f.Close()

	re := &io.LimitedReader{
		N: sizeLimit + 1,
		R: f,
	}

	data, err := io.ReadAll(re)
	if err != nil {
		return nil, err
	}

	if re.N == 0 {
		return nil, errTooLarge
	}

	secret := &Secret{}

	if err = secret.UnmarshalText(data); err != nil {
		return nil, fmt.Errorf("decoding: %w", err)
	}

	return secret, nil
}
