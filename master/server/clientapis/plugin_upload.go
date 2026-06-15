package clientapis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mapreduce/master/scheduler"
	"mapreduce/rpc/comm"
	clientcall "mapreduce/rpc/master/client-call"
	"mapreduce/tool"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/gin-gonic/gin"
)

const randomPluginNameSuffixBytes = 32

const validatePluginTimeout = 10 * time.Second

func validatePluginLoadable(path string) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), validatePluginTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, exe, "check-plugin-loadable", path)
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("plugin validation timed out")
	}
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg == "" {
			return err
		}
		return fmt.Errorf("%v: %s", err, msg)
	}
	return nil
}

func UploadPlugin(pluginStorePath string) gin.HandlerFunc {
	return func(c *gin.Context) {
		pluginName := sanitizePathPart(c.Param("plugin_name"))
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

		uploadTmpFileName := tool.GitLikeRandomHex(32) + ".uploading"

		uploadTmpFIlePath, ok := pluginPath(pluginStorePath, uploadTmpFileName)
		if !ok {
			panic("unexpected")
		}

		// tmpPath := path + ".tmp"
		uploadTmpFile, err := os.OpenFile(uploadTmpFIlePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			c.JSON(http.StatusInternalServerError, comm.RespComm{
				Code: comm.CodeInternalError,
				Msg:  comm.GetMsgFromCode(comm.CodeInternalError),
			})
			return
		}
		defer uploadTmpFile.Close()
		defer os.Remove(uploadTmpFIlePath)

		_, copyErr := io.Copy(uploadTmpFile, c.Request.Body)
		closeErr := uploadTmpFile.Close()
		if copyErr != nil || closeErr != nil {
			c.JSON(http.StatusInternalServerError, comm.RespComm{
				Code: comm.CodeInternalError,
				Msg:  comm.GetMsgFromCode(comm.CodeInternalError),
			})
			return
		}

		if err := validatePluginLoadable(uploadTmpFIlePath); err != nil {
			c.JSON(http.StatusBadRequest, comm.RespComm{
				Code: comm.CodeBadRequest,
				Msg:  "cannot load plugin: " + err.Error(),
			})
			return
		}

		pluginUniqueID := scheduler.GetScheduler().RegisterPlugin(pluginName)
		if pluginUniqueID == "" {
			c.JSON(http.StatusInternalServerError, comm.RespComm{
				Code: comm.CodeInternalError,
				Msg:  comm.GetMsgFromCode(comm.CodeInternalError),
			})
			return
		}
		pluginPath, ok := pluginPath(pluginStorePath, pluginUniqueID)
		if !ok {
			panic("unexpected")
		}
		if err := os.Rename(uploadTmpFIlePath, pluginPath); err != nil {
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
			PluginUniqueID: pluginUniqueID,
		})
	}
}

func generatePluginUniqueIDAndReturnsPath(pluginStorePath string, pluginUniqueIDPrefix string) (string, string, error) {
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

func pluginPath(pluginStorePath string, fileName string) (string, bool) {
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
