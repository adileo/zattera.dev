package secrets

import "fmt"

// Exported argon2id parameters, reused by password hashing (T-04) so it stays
// in lockstep with the cluster-key KDF.
const (
	ArgonTime      = argonTime
	ArgonMemoryKiB = argonMemoryKiB
	ArgonThreads   = argonThreads
	ArgonKeyLen    = keyLen
	ArgonSaltLen   = saltLen
)

// Keyring holds the cluster data key in memory only. Control nodes keep it for
// the lifetime of the process; it is never written to disk. A joining control
// node receives it from the leader over mTLS (M2); a restore from backup
// derives it from the recovery passphrase.
type Keyring struct {
	dataKey    []byte
	keyVersion uint32
}

// NewKeyring wraps a plaintext data key. It copies the key so the caller may
// zero its own buffer.
func NewKeyring(dataKey []byte, keyVersion uint32) (*Keyring, error) {
	if len(dataKey) != keyLen {
		return nil, fmt.Errorf("secrets: data key must be %d bytes, got %d", keyLen, len(dataKey))
	}
	cp := append([]byte(nil), dataKey...)
	return &Keyring{dataKey: cp, keyVersion: keyVersion}, nil
}

// Sealer returns a Sealer bound to the current data key version.
func (k *Keyring) Sealer() (Sealer, error) {
	return NewSealer(k.dataKey, k.keyVersion)
}

// DataKey returns a copy of the plaintext data key (for handing to a joining
// control node over mTLS). Handle with care; never log or persist it.
func (k *Keyring) DataKey() []byte {
	return append([]byte(nil), k.dataKey...)
}

// KeyVersion returns the data key version.
func (k *Keyring) KeyVersion() uint32 { return k.keyVersion }
