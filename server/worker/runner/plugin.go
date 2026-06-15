package runner

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	mrplugin "mapreduce/plugin"
	workercall "mapreduce/rpc/master/worker-call"
	"mapreduce/server/worker/entity"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	stdplugin "plugin"
	"strings"
)

func loadPlugin(path string, confValues map[string]string) (*loadedPlugin, error) {
	opened, err := stdplugin.Open(path)
	if err != nil {
		return nil, err
	}
	sym, err := opened.Lookup("BuildPlugin")
	if err != nil {
		return nil, err
	}
	build, ok := sym.(func([]string) (mrplugin.Configuration, mrplugin.JobPlugin, error))
	if !ok {
		return nil, fmt.Errorf("BuildPlugin has unexpected signature")
	}
	conf, jobPlugin, err := build(nil)
	if err != nil {
		return nil, err
	}
	if conf == nil {
		conf = mrplugin.NewConfiguration()
	}
	for key, value := range confValues {
		if err := conf.Set(key, value); err != nil {
			return nil, err
		}
	}
	return &loadedPlugin{Conf: conf, Plugin: jobPlugin}, nil
}

func materializePlugin(spec TaskSpec) (string, error) {
	if spec.Assign.Plugin.Type == entity.FromLocalFS && spec.Assign.Plugin.URI != "" {
		if err := verifyPluginFileSHA256(spec.Assign.Plugin.URI, spec.Assign.Plugin.SHA256); err != nil {
			return "", err
		}
		return spec.Assign.Plugin.URI, nil
	}
	pluginUniqueID := spec.Assign.Plugin.PluginUniqueID
	if pluginUniqueID == "" {
		return "", fmt.Errorf("plugin unique id is empty")
	}
	pluginBytes, err := fetchPluginFromMaster(spec.MasterAddr, pluginUniqueID)
	if err != nil {
		return "", err
	}
	if err := verifyBytesSHA256(pluginBytes, spec.Assign.Plugin.SHA256); err != nil {
		return "", err
	}
	pluginPath := filepath.Join(spec.AttemptDir, "plugin.so")
	if err := os.WriteFile(pluginPath, pluginBytes, 0o644); err != nil {
		return "", err
	}
	return pluginPath, nil
}

func fetchPluginFromMaster(masterAddr string, pluginUniqueID string) ([]byte, error) {
	base := strings.TrimRight(masterAddr, "/")
	if base != "" && !strings.Contains(base, "://") {
		base = "http://" + base
	}
	resp, err := http.Get(base + "/client-api/fetch-plugin/" + url.PathEscape(pluginUniqueID))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	if resp.StatusCode >= 400 {
		var failure workercall.FetchPluginByJobIDRespOnFailure
		_ = json.Unmarshal(data, &failure)
		if failure.Code != 0 {
			return nil, fmt.Errorf("fetch plugin failed: http=%d code=%d msg=%s", resp.StatusCode, failure.Code, failure.Msg)
		}
		return nil, fmt.Errorf("fetch plugin failed: http=%d", resp.StatusCode)
	}
	return data, nil
}

func verifyPluginFileSHA256(path string, expected string) error {
	if expected == "" {
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return err
	}
	actual := fmt.Sprintf("%x", hash.Sum(nil))
	if actual != expected {
		return fmt.Errorf("plugin sha256 mismatch: actual=%s expected=%s", actual, expected)
	}
	return nil
}

func verifyBytesSHA256(data []byte, expected string) error {
	if expected == "" {
		return nil
	}
	actual := fmt.Sprintf("%x", sha256.Sum256(data))
	if actual != expected {
		return fmt.Errorf("plugin sha256 mismatch: actual=%s expected=%s", actual, expected)
	}
	return nil
}
