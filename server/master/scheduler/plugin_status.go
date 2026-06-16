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
	// FilePath       string
	ModTime   time.Time
	DeletedAt *time.Time
	Pin       map[string]struct{}
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

const (
	deletedPluginMarkerSuffix = ".deleted"
	uploadingPluginSuffix     = ".uploading"
)

func LoadPluginStatusesFromStore(pluginStorePath string, cleanupAuxiliaryFiles bool) ([]pluginStatus, error) {
	entries, err := os.ReadDir(pluginStorePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	plugins := make([]pluginStatus, 0, len(entries))
	removedPlugins := make(map[string]struct{})
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if cleanupAuxiliaryFiles && strings.HasSuffix(name, uploadingPluginSuffix) {
			if err := removePluginStoreFile(pluginStorePath, name); err != nil {
				return nil, err
			}
			continue
		}
		if cleanupAuxiliaryFiles && strings.HasSuffix(name, deletedPluginMarkerSuffix) {
			pluginName := strings.TrimSuffix(name, deletedPluginMarkerSuffix)
			if err := removeDeletedPluginFiles(pluginStorePath, pluginName); err != nil {
				return nil, err
			}
			removedPlugins[pluginName] = struct{}{}
			continue
		}
		if isPluginAuxiliaryFile(name) {
			continue
		}
		if _, removed := removedPlugins[name]; removed {
			continue
		}
		path, ok := safePluginPath(pluginStorePath, name)
		if !ok {
			continue
		}
		var deletedAt *time.Time
		markerInfo, err := os.Stat(deletedPluginMarkerPath(path))
		if err == nil {
			if cleanupAuxiliaryFiles {
				if err := removeDeletedPluginFiles(pluginStorePath, name); err != nil {
					return nil, err
				}
				removedPlugins[name] = struct{}{}
				continue
			}
			markerModTime := markerInfo.ModTime()
			deletedAt = &markerModTime
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		plugins = append(plugins, pluginStatus{
			PluginUniqueID: name,
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

func safePluginPath(pluginStorePath string, pluginUniqueID string) (string, bool) {
	if pluginUniqueID == "" || filepath.Base(pluginUniqueID) != pluginUniqueID {
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

func pluginVisible(plugin *pluginStatus) bool {
	return plugin != nil && plugin.DeletedAt == nil
}

func deletedPluginMarkerPath(pluginFilePath string) string {
	return pluginFilePath + deletedPluginMarkerSuffix
}

func removePluginStoreFile(pluginStorePath string, fileName string) error {
	path, ok := safePluginPath(pluginStorePath, fileName)
	if !ok {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func removeDeletedPluginFiles(pluginStorePath string, pluginUniqueID string) error {
	path, ok := safePluginPath(pluginStorePath, pluginUniqueID)
	if !ok {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Remove(deletedPluginMarkerPath(path)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func isPluginAuxiliaryFile(fileName string) bool {
	return strings.Contains(fileName, ".tmp-") ||
		strings.HasSuffix(fileName, deletedPluginMarkerSuffix) ||
		strings.HasSuffix(fileName, uploadingPluginSuffix)
}
