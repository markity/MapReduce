package scheduler

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadPluginStatusesFromStorePreservesDeletedWhenCleanupDisabled(t *testing.T) {
	dir := t.TempDir()
	pluginPath := filepath.Join(dir, "plugin-a")
	if err := os.WriteFile(pluginPath, []byte("plugin"), 0o644); err != nil {
		t.Fatalf("write plugin failed: %v", err)
	}
	if err := os.WriteFile(deletedPluginMarkerPath(pluginPath), []byte("deleted"), 0o644); err != nil {
		t.Fatalf("write deleted marker failed: %v", err)
	}

	plugins, err := LoadPluginStatusesFromStore(dir, false)
	if err != nil {
		t.Fatalf("LoadPluginStatusesFromStore failed: %v", err)
	}
	if len(plugins) != 1 {
		t.Fatalf("plugins len = %d, want 1", len(plugins))
	}
	if plugins[0].PluginUniqueID != "plugin-a" {
		t.Fatalf("plugin id = %q, want plugin-a", plugins[0].PluginUniqueID)
	}
	if plugins[0].DeletedAt == nil {
		t.Fatalf("DeletedAt is nil, want deleted marker restored")
	}
	if _, err := os.Stat(pluginPath); err != nil {
		t.Fatalf("plugin file should remain: %v", err)
	}
	if _, err := os.Stat(deletedPluginMarkerPath(pluginPath)); err != nil {
		t.Fatalf("deleted marker should remain: %v", err)
	}
}

func TestLoadPluginStatusesFromStoreCleansAuxiliaryFilesWhenEnabled(t *testing.T) {
	dir := t.TempDir()
	activePath := filepath.Join(dir, "plugin-active")
	deletedPath := filepath.Join(dir, "plugin-deleted")
	uploadingPath := filepath.Join(dir, "plugin-upload.uploading")
	for _, path := range []string{activePath, deletedPath, uploadingPath} {
		if err := os.WriteFile(path, []byte("plugin"), 0o644); err != nil {
			t.Fatalf("write %s failed: %v", path, err)
		}
	}
	if err := os.WriteFile(deletedPluginMarkerPath(deletedPath), []byte("deleted"), 0o644); err != nil {
		t.Fatalf("write deleted marker failed: %v", err)
	}

	plugins, err := LoadPluginStatusesFromStore(dir, true)
	if err != nil {
		t.Fatalf("LoadPluginStatusesFromStore failed: %v", err)
	}
	if len(plugins) != 1 {
		t.Fatalf("plugins len = %d, want 1", len(plugins))
	}
	if plugins[0].PluginUniqueID != "plugin-active" {
		t.Fatalf("plugin id = %q, want plugin-active", plugins[0].PluginUniqueID)
	}
	for _, path := range []string{deletedPath, deletedPluginMarkerPath(deletedPath), uploadingPath} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("%s should be removed, stat err=%v", path, err)
		}
	}
	if _, err := os.Stat(activePath); err != nil {
		t.Fatalf("active plugin should remain: %v", err)
	}
}
