package server

import (
	"mapreduce/server/master/server/workerapis"
)

func (s *Server) registerWorkerApi() {
	workerApi := s.engine.Group("/worker-api")
	{
		workerApi.POST("/heartbeat", workerapis.Heartbeat())
		workerApi.GET("/fetch-job-plugin/:job_id", workerapis.FetchPlugin())
	}
}
