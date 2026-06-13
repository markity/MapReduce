package plugin

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"sync"
)

type configuration struct {
	mu     sync.RWMutex
	values map[string]string
}

type Configuration interface {
	Set(key string, val string) error
	Get(key string) (string, bool)
	GetInt(key string, defaultValue int) int
	GetInt64(key string, defaultValue int64) int64
	GetBool(key string, defaultValue bool) bool
	GetStringSlice(key string) []string
}

func (c *configuration) Set(key string, val string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.values[key] = val

	return nil
}

func (c *configuration) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	cv, ok := c.values[key]
	if !ok {
		return "", false
	}

	return c.expandLocked(cv, 0), true
}

func (c *configuration) expandLocked(value string, depth int) string {
	if depth > 20 {
		return value
	}

	start := strings.Index(value, "${")
	if start < 0 {
		return value
	}

	end := strings.Index(value[start:], "}")
	if end < 0 {
		return value
	}

	end = start + end

	expr := value[start+2 : end]
	replacement := ""

	if strings.HasPrefix(expr, "env.") {
		envKey := strings.TrimPrefix(expr, "env.")
		replacement = os.Getenv(envKey)
	} else if cv, ok := c.values[expr]; ok {
		replacement = c.expandLocked(cv, depth+1)
	}

	next := value[:start] + replacement + value[end+1:]
	return c.expandLocked(next, depth+1)
}

func (c *configuration) GetInt(key string, defaultValue int) int {
	raw, ok := c.Get(key)
	if !ok {
		return defaultValue
	}

	v, err := strconv.Atoi(raw)
	if err != nil {
		return defaultValue
	}

	return v
}

func (c *configuration) GetInt64(key string, defaultValue int64) int64 {
	raw, ok := c.Get(key)
	if !ok {
		return defaultValue
	}

	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return defaultValue
	}

	return v
}

func (c *configuration) GetBool(key string, defaultValue bool) bool {
	raw, ok := c.Get(key)
	if !ok {
		return defaultValue
	}

	v, err := strconv.ParseBool(raw)
	if err != nil {
		return defaultValue
	}

	return v
}

func (c *configuration) GetStringSlice(key string) []string {
	raw, ok := c.Get(key)
	if !ok {
		return nil
	}

	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))

	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}

	return out
}

func (c *configuration) ToJSONBytes() ([]byte, error) {
	return json.Marshal(c.values)
}

func NewConfiguration() Configuration {
	return &configuration{
		values: make(map[string]string),
	}
}
