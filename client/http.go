package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mapreduce/rpc/comm"
	"net/http"
	"strings"
)

func doJSON(method string, path string, body []byte, out any) error {
	req, err := http.NewRequest(method, endpoint(path), bytes.NewReader(body))
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if len(data) != 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("decode response: %w: %s", err, string(data))
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("request failed: http=%d\n%s", resp.StatusCode, string(data))
	}
	if code, ok := responseCode(out); ok && code != comm.CodeOK {
		return fmt.Errorf("request failed: code=%d msg=%s", code, comm.GetMsgFromCode(code))
	}
	return nil
}

func endpoint(path string) string {
	base := strings.TrimRight(masterAddr, "/")
	if base != "" && !strings.Contains(base, "://") {
		base = "http://" + base
	}
	return base + path
}

func responseCode(v any) (comm.Code, bool) {
	data, err := json.Marshal(v)
	if err != nil {
		return 0, false
	}
	var resp comm.RespComm
	if err := json.Unmarshal(data, &resp); err != nil {
		return 0, false
	}
	return resp.Code, true
}
