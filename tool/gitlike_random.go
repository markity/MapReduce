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

var used map[string]struct{} = make(map[string]struct{})

func StrictGitLikeRandomHex(length int, try int) (string, bool) {
	for i := 0; i < try; i++ {
		generated := GitLikeRandomHex(length)
		if _, ok := used[generated]; ok {
			continue
		}

		used[generated] = struct{}{}
		return generated, true
	}
	return "", false
}
