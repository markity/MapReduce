package scheduler

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type pluginStatus struct {
	PluginUniqueID string
	FilePath       string
	ModTime        time.Time
	DeletedAt      *time.Time
	Pin            map[string]struct{}
}

type PluginSnapshot struct {
	PluginUniqueID string
	ModTime        time.Time
}

func pluginSnapshotFromStatus(plugin *pluginStatus) PluginSnapshot {
	return PluginSnapshot{
		PluginUniqueID: plugin.PluginUniqueID,
		ModTime:        plugin.ModTime,
	}
}

const deletedPluginMarkerSuffix = ".deleted"

func LoadPluginStatusesFromStore(pluginStorePath string) ([]pluginStatus, error) {
	entries, err := os.ReadDir(pluginStorePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	plugins := make([]pluginStatus, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || isPluginAuxiliaryFile(entry.Name()) {
			continue
		}
		path, ok := safePluginPath(pluginStorePath, entry.Name())
		if !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		var deletedAt *time.Time
		markerInfo, err := os.Stat(deletedPluginMarkerPath(path))
		if err == nil {
			markerModTime := markerInfo.ModTime()
			deletedAt = &markerModTime
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		plugins = append(plugins, pluginStatus{
			PluginUniqueID: entry.Name(),
			FilePath:       path,
			ModTime:        info.ModTime(),
			DeletedAt:      deletedAt,
			Pin:            make(map[string]struct{}),
		})
	}
	return plugins, nil
}

func pluginStatusMapFromSlice(plugins []pluginStatus) map[string]*pluginStatus {
	result := make(map[string]*pluginStatus)
	for _, plugin := range plugins {
		pluginCopy := plugin
		result[plugin.PluginUniqueID] = &pluginCopy
	}
	return result
}

func safePluginPath(pluginStorePath string, pluginName string) (string, bool) {
	if pluginName == "" || filepath.Base(pluginName) != pluginName {
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

func pluginVisible(plugin *pluginStatus) bool {
	return plugin != nil && plugin.DeletedAt == nil
}

func deletedPluginMarkerPath(pluginFilePath string) string {
	return pluginFilePath + deletedPluginMarkerSuffix
}

func isPluginAuxiliaryFile(fileName string) bool {
	return strings.Contains(fileName, ".tmp-") || strings.HasSuffix(fileName, deletedPluginMarkerSuffix)
}
