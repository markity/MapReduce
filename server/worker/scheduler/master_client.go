package scheduler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mapreduce/rpc/comm"
	workercall "mapreduce/rpc/master/worker-call"
	"net/http"
	"strings"
	"time"
)

type masterClient interface {
	Heartbeat(req workercall.HeartbeatReq) (*workercall.HeartbeatResp, error)
}

type httpMasterClient struct {
	BaseURL string
	Client  *http.Client
}

func newHTTPMasterClient(baseURL string) *httpMasterClient {
	baseURL = strings.TrimRight(baseURL, "/")
	if baseURL != "" && !strings.Contains(baseURL, "://") {
		baseURL = "http://" + baseURL
	}
	return &httpMasterClient{
		BaseURL: baseURL,
		Client:  &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *httpMasterClient) Heartbeat(req workercall.HeartbeatReq) (*workercall.HeartbeatResp, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpResp, err := c.client().Post(c.endpoint("/worker-api/heartbeat"), "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	var resp workercall.HeartbeatResp
	if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		return nil, err
	}
	if httpResp.StatusCode >= 400 {
		return &resp, fmt.Errorf("heartbeat failed: http=%d code=%d msg=%s", httpResp.StatusCode, resp.Code, resp.Msg)
	}
	return &resp, nil
}

func (c *httpMasterClient) client() *http.Client {
	if c.Client != nil {
		return c.Client
	}
	return http.DefaultClient
}

func (c *httpMasterClient) endpoint(path string) string {
	return c.BaseURL + path
}

func okCode(code comm.Code) bool {
	return code == comm.CodeOK
}
