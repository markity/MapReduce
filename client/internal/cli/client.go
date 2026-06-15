package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mapreduce/rpc/comm"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	MasterAddr string
	HTTPClient *http.Client
}

func NewClient(masterAddr string) *Client {
	return &Client{
		MasterAddr: masterAddr,
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) DoJSON(method string, path string, body []byte, out any) error {
	req, err := http.NewRequest(method, c.endpoint(path), bytes.NewReader(body))
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.doJSONResponse(req, out)
}

func (c *Client) DoBinary(method string, path string, body []byte, out any) error {
	req, err := http.NewRequest(method, c.endpoint(path), bytes.NewReader(body))
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	return c.doJSONResponse(req, out)
}

func (c *Client) DoBytes(method string, path string, body []byte) ([]byte, error) {
	req, err := http.NewRequest(method, c.endpoint(path), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var failure comm.RespComm
		if err := json.Unmarshal(data, &failure); err == nil && failure.Code != 0 {
			return nil, fmt.Errorf("request failed: http=%d code=%d msg=%s", resp.StatusCode, failure.Code, failure.Msg)
		}
		return nil, fmt.Errorf("request failed: http=%d\n%s", resp.StatusCode, string(data))
	}
	return data, nil
}

func (c *Client) doJSONResponse(req *http.Request, out any) error {
	resp, err := c.HTTPClient.Do(req)
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

func (c *Client) endpoint(path string) string {
	base := strings.TrimRight(c.MasterAddr, "/")
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
