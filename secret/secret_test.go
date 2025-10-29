package secret_test

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ninedraft/tgbotstream/secret"
)

func TestSecretEqual(t *testing.T) {
	t.Parallel()

	tc := func(name string, build func() (left, right *secret.Secret), expect bool) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			left, right := build()

			if left == nil {
				require.Equal(t, expect, (*secret.Secret)(nil).Equal(right))
			} else {
				require.Equal(t, expect, left.Equal(right))
			}

			if right == nil {
				require.Equal(t, expect, (*secret.Secret)(nil).Equal(left))
			} else {
				require.Equal(t, expect, right.Equal(left))
			}
		})
	}

	tc("both nil", func() (*secret.Secret, *secret.Secret) {
		return nil, nil
	}, true)

	tc("same pointer", func() (*secret.Secret, *secret.Secret) {
		s := secret.New([]byte("user"), []byte("pass"))
		return s, s
	}, true)

	tc("identical values", func() (*secret.Secret, *secret.Secret) {
		return secret.New([]byte("user"), []byte("pass")), secret.New([]byte("user"), []byte("pass"))
	}, true)

	tc("different username", func() (*secret.Secret, *secret.Secret) {
		return secret.New([]byte("user"), []byte("pass")), secret.New([]byte("other"), []byte("pass"))
	}, false)

	tc("different password", func() (*secret.Secret, *secret.Secret) {
		return secret.New([]byte("user"), []byte("pass")), secret.New([]byte("user"), []byte("secret"))
	}, false)

	tc("left nil right non-nil", func() (*secret.Secret, *secret.Secret) {
		return nil, secret.New([]byte("user"), []byte("pass"))
	}, false)

	tc("left non-nil right nil", func() (*secret.Secret, *secret.Secret) {
		return secret.New([]byte("user"), []byte("pass")), nil
	}, false)
}

func TestSecretUnmarshalText(t *testing.T) {
	t.Parallel()

	tc := func(name, input string, expect *secret.Secret, wantErr bool) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var got secret.Secret
			err := got.UnmarshalText([]byte(input))

			if wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, expect)
			require.True(t, (&got).Equal(expect))
			require.True(t, expect.Equal(&got))
		})
	}

	tc("valid input trimmed", " user:test-pass\n", secret.New([]byte("user"), []byte("test-pass")), false)
	tc("missing separator", "onlyusername", nil, true)
}

func TestSecretEncode(t *testing.T) {
	t.Parallel()

	const separator = ":"

	tc := func(name string, username, password []byte) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			usernameCopy := append([]byte(nil), username...)
			passwordCopy := append([]byte(nil), password...)

			secretOriginal := secret.New(usernameCopy, passwordCopy)
			encoded := secretOriginal.Encode()

			expectedEncodedLen := base64.StdEncoding.EncodedLen(len(username)) +
				base64.StdEncoding.EncodedLen(len(separator)) +
				base64.StdEncoding.EncodedLen(len(password))
			require.Len(t, encoded, expectedEncodedLen)

			offset := 0
			nextChunk := func(length int) string {
				start := offset
				end := offset + length
				require.LessOrEqual(t, end, len(encoded))
				offset = end
				return encoded[start:end]
			}

			decodeChunk := func(length int) []byte {
				decoded, err := base64.StdEncoding.DecodeString(nextChunk(length))
				require.NoError(t, err)
				return decoded
			}

			rawUsername := decodeChunk(base64.StdEncoding.EncodedLen(len(username)))
			rawSeparator := decodeChunk(base64.StdEncoding.EncodedLen(len(separator)))
			rawPassword := decodeChunk(base64.StdEncoding.EncodedLen(len(password)))
			require.Equal(t, separator, string(rawSeparator))
			require.Equal(t, len(encoded), offset)

			raw := append([]byte{}, rawUsername...)
			raw = append(raw, rawSeparator...)
			raw = append(raw, rawPassword...)
			require.Equal(t, string(username)+separator+string(password), string(raw))

			var parsed secret.Secret
			require.NoError(t, parsed.UnmarshalText(raw))
			require.True(t, secretOriginal.Equal(&parsed))
			require.True(t, (&parsed).Equal(secretOriginal))
		})
	}

	tc("alpha numeric", []byte("user123"), []byte("pass456!"))
	tc("with spaces", []byte("user name"), []byte("p@$$ word"))
}
