package clientapis

import (
	"errors"
	"io"
	"mapreduce/master/scheduler"
	"mapreduce/rpc/comm"
	clientcall "mapreduce/rpc/master/client-call"
	"mapreduce/tool"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/gin-gonic/gin"
)

const randomPluginNameSuffixBytes = 32

func UploadPlugin(pluginStorePath string) gin.HandlerFunc {
	return func(c *gin.Context) {
		pluginName := sanitizePluginName(c.Param("plugin_name"))
		if pluginName == "" {
			c.JSON(http.StatusBadRequest, comm.RespComm{
				Code: comm.CodeBadRequest,
				Msg:  comm.GetMsgFromCode(comm.CodeBadRequest),
			})
			return
		}

		if err := os.MkdirAll(pluginStorePath, 0o755); err != nil {
			c.JSON(http.StatusInternalServerError, comm.RespComm{
				Code: comm.CodeInternalError,
				Msg:  comm.GetMsgFromCode(comm.CodeInternalError),
			})
			return
		}

		generatedPluginUniqueID, path, err := newPluginPath(pluginStorePath, pluginName)
		if err != nil {
			c.JSON(http.StatusInternalServerError, comm.RespComm{
				Code: comm.CodeInternalError,
				Msg:  comm.GetMsgFromCode(comm.CodeInternalError),
			})
			return
		}

		tmpPath := path + ".tmp"
		file, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			c.JSON(http.StatusInternalServerError, comm.RespComm{
				Code: comm.CodeInternalError,
				Msg:  comm.GetMsgFromCode(comm.CodeInternalError),
			})
			return
		}

		_, copyErr := io.Copy(file, c.Request.Body)
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil {
			_ = os.Remove(tmpPath)
			c.JSON(http.StatusInternalServerError, comm.RespComm{
				Code: comm.CodeInternalError,
				Msg:  comm.GetMsgFromCode(comm.CodeInternalError),
			})
			return
		}

		if err := validatePluginLoadable(tmpPath); err != nil {
			_ = os.Remove(tmpPath)
			c.JSON(http.StatusBadRequest, comm.RespComm{
				Code: comm.CodeBadRequest,
				Msg:  "cannot load plugin",
			})
			return
		}

		if err := os.Rename(tmpPath, path); err != nil {
			_ = os.Remove(tmpPath)
			c.JSON(http.StatusInternalServerError, comm.RespComm{
				Code: comm.CodeInternalError,
				Msg:  comm.GetMsgFromCode(comm.CodeInternalError),
			})
			return
		}
		if !scheduler.GetScheduler().RegisterPlugin(generatedPluginUniqueID, path) {
			_ = os.Remove(path)
			c.JSON(http.StatusInternalServerError, comm.RespComm{
				Code: comm.CodeInternalError,
				Msg:  comm.GetMsgFromCode(comm.CodeInternalError),
			})
			return
		}

		c.JSON(http.StatusOK, clientcall.UploadPluginResp{
			RespComm: comm.RespComm{
				Code: comm.CodeOK,
				Msg:  comm.GetMsgFromCode(comm.CodeOK),
			},
			PluginUniqueID: generatedPluginUniqueID,
		})
	}
}

func newPluginPath(pluginStorePath string, pluginName string) (string, string, error) {
	for i := 0; i < 16; i++ {
		generatedPluginName := pluginName + "-" + tool.GitLikeRandomHex(32)
		path, ok := pluginPath(pluginStorePath, generatedPluginName)
		if !ok {
			return "", "", errors.New("generated invalid plugin name")
		}
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return generatedPluginName, path, nil
		} else if err != nil {
			return "", "", err
		}
	}
	return "", "", errors.New("failed to allocate unique plugin name")
}

func pluginPath(pluginStorePath string, pluginName string) (string, bool) {
	if sanitizePluginName(pluginName) != pluginName {
		return "", false
	}
	path := filepath.Join(pluginStorePath, pluginName)
	cleanStorePath := filepath.Clean(pluginStorePath)
	cleanPath := filepath.Clean(path)
	if cleanPath != filepath.Join(cleanStorePath, filepath.Base(cleanPath)) {
		return "", false
	}
	return cleanPath, true
}

func sanitizePluginName(pluginName string) string {
	pluginName = strings.TrimSpace(pluginName)
	if pluginName == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range pluginName {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r), r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return strings.Trim(b.String(), ".-_")
}
