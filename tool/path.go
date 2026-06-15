package tool

import (
	"path/filepath"
	"strings"
	"unicode"
)

func sanitizePathPart(part string) string {
	part = strings.TrimSpace(part)
	if part == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range part {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r), r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return strings.Trim(b.String(), ".-_")
}

func isSafePathPart(part string) bool {
	return part != "" && sanitizePathPart(part) == part
}

func JoinPathPath(pluginStorePath string, fileName string) (string, bool) {
	if !isSafePathPart(fileName) {
		return "", false
	}
	path := filepath.Join(pluginStorePath, fileName)
	cleanStorePath := filepath.Clean(pluginStorePath)
	cleanPath := filepath.Clean(path)
	if cleanPath != filepath.Join(cleanStorePath, filepath.Base(cleanPath)) {
		return "", false
	}
	return cleanPath, true
}
