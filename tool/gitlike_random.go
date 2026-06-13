package tool

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"time"
)

func GitLikeRandomHex(length int) string {
	randomBytes := make([]byte, length)
	if _, err := rand.Read(randomBytes); err != nil {
		now := time.Now().Format(time.RFC3339Nano)
		randomBytes = []byte(now)
	}
	sum := sha1.Sum(randomBytes)
	return hex.EncodeToString(sum[:])
}
