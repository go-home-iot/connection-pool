package pool

import (
	"net"
	"time"
)

// Config contains all of the configuration parameters for the connection pool
type Config struct {
	Name          string
	Size          int
	Address       string
	RetryDuration time.Duration
	NewConnection func(Config) (net.Conn, error)
}
