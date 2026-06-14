package server

import (
	"mapreduce/master/server/clientapis"
)

func (s *Server) registerClientApi() {
	clientApi := s.engine.Group("/client-api")
	{
		// 上传插件
		clientApi.POST("/upload-plugin/:plugin_name", clientapis.UploadPlugin(s.pluginStorePath))

		// 删除插件
		clientApi.DELETE("/plugin/:plugin_unique_id", clientapis.DeletePlugin(s.pluginStorePath))

		// 下载插件
		clientApi.GET("/fetch-plugin/:plugin_unique_id", clientapis.FetchPlugin())

		// 获取插件列表
		clientApi.GET("/plugins/:order", clientapis.ListPlugins())

		// 获取任务列表
		clientApi.GET("/jobs", clientapis.ListJobs())

		// 获取任务详情
		clientApi.GET("/jobs/:id", clientapis.GetJob())

		// 提交任务
		clientApi.POST("/jobs", clientapis.CreateMapReduceJob())

		// 获取master内部状态机快照
		clientApi.GET("/master-state", clientapis.GetMasterState())
	}
}
