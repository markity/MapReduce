package server

import (
	"log"
	"mapreduce/master/scheduler"
	"time"

	"github.com/gin-gonic/gin"
)

type Server struct {
	engine                       *gin.Engine
	addr                         string
	pluginStorePath              string
	pluginCleanupIntervalSeconds int
}

func NewServer(addr string, pluginStorePath string, pluginCleanupIntervalSeconds int, workerHeartbeatLostIntervalSeconds int) *Server {
	engine := gin.Default()
	plugins, err := scheduler.LoadPluginStatusesFromStore(pluginStorePath)
	if err != nil {
		log.Printf("load plugins from %s failed: %v", pluginStorePath, err)
	}
	scheduler.InitScheduler(plugins, workerHeartbeatLostIntervalSeconds)
	s := &Server{
		engine:                       engine,
		addr:                         addr,
		pluginStorePath:              pluginStorePath,
		pluginCleanupIntervalSeconds: pluginCleanupIntervalSeconds,
	}

	// 注册路由
	s.registerRoutes()
	s.startPluginCleanupLoop()

	return s
}

func (s *Server) registerRoutes() {
	// health check
	s.engine.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status": "ok",
		})
	})

	// worker call
	s.registerWorkerApi()
	// client call
	s.registerClientApi()
}

func (s *Server) Start() error {
	return s.engine.Run(s.addr)
}

func (s *Server) startPluginCleanupLoop() {
	if s.pluginCleanupIntervalSeconds <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(time.Duration(s.pluginCleanupIntervalSeconds) * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if err := scheduler.GetScheduler().CleanupDeletedPlugins(); err != nil {
				log.Printf("cleanup deleted plugins failed: %v", err)
			}
		}
	}()
}
