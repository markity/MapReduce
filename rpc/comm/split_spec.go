package comm

import (
	"encoding/json"
)

type SplitType string

type SplitSpec struct {
	SplitType SplitType       `json:"type"`
	Data      json.RawMessage `json:"data"`
}
