package entity

type PluginSpecType string

const (
	// 在worker自己本地文件系统
	FromLocalFS PluginSpecType = "from-local-fs"
	// 使用api从master拉
	FromMaster PluginSpecType = "from-master"
)

type PluginSpec struct {
	Type   PluginSpecType `json:"type"`
	URI    string         `json:"uri"`
	SHA256 string         `json:"sha256"`
}
