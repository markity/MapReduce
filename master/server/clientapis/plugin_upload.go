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
		pluginUniqueIDPrefix := sanitizePathPart(c.Param("plugin_name"))
		if pluginUniqueIDPrefix == "" {
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

		generatedPluginUniqueID, path, err := newPluginPath(pluginStorePath, pluginUniqueIDPrefix)
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
				Msg:  "cannot load plugin: " + err.Error(),
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

func newPluginPath(pluginStorePath string, pluginUniqueIDPrefix string) (string, string, error) {
	for i := 0; i < 16; i++ {
		generatedPluginUniqueID := pluginUniqueIDPrefix + "-" + tool.GitLikeRandomHex(32)
		path, ok := pluginPath(pluginStorePath, generatedPluginUniqueID)
		if !ok {
			return "", "", errors.New("generated invalid plugin unique id")
		}
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return generatedPluginUniqueID, path, nil
		} else if err != nil {
			return "", "", err
		}
	}
	return "", "", errors.New("failed to allocate plugin unique id")
}

func pluginPath(pluginStorePath string, pluginUniqueID string) (string, bool) {
	if !isSafePathPart(pluginUniqueID) {
		return "", false
	}
	path := filepath.Join(pluginStorePath, pluginUniqueID)
	cleanStorePath := filepath.Clean(pluginStorePath)
	cleanPath := filepath.Clean(path)
	if cleanPath != filepath.Join(cleanStorePath, filepath.Base(cleanPath)) {
		return "", false
	}
	return cleanPath, true
}

func isSafePathPart(part string) bool {
	return part != "" && sanitizePathPart(part) == part
}

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
