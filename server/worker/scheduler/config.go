package scheduler

import "time"

type Config struct {
	MasterAddr        string
	ListenAddr        string
	AdvertiseAddr     string
	WorkerUniqueID    string
	WorkerEpoch       int64
	DataDir           string
	NumSlots          int
	HeartbeatInterval time.Duration
	MapSpillBufferMB  int
	RunTaskArgs       []string
}

func (c Config) normalized() Config {
	if c.NumSlots <= 0 {
		c.NumSlots = 1
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = time.Second
	}
	if c.AdvertiseAddr == "" {
		c.AdvertiseAddr = c.ListenAddr
	}
	if c.MapSpillBufferMB <= 0 {
		c.MapSpillBufferMB = 64
	}
	return c
}

func (c Config) MapSpillBufferBytes() int64 {
	return int64(c.MapSpillBufferMB) << 20
}

func (c Config) NormalizedForCmd() Config {
	return c.normalized()
}
