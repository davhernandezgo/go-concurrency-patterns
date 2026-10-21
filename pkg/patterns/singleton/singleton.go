package singleton

import (
	"sync"
	"time"
)

// DatabaseConnection represents a shared singleton resource.
type DatabaseConnection struct {
	ID        string
	CreatedAt time.Time
}

var (
	instance *DatabaseConnection
	once     sync.Once
)

// GetInstance returns the thread-safe singleton DatabaseConnection instance initialized once.
func GetInstance() *DatabaseConnection {
	once.Do(func() {
		instance = &DatabaseConnection{
			ID:        "db-pool-primary-node-01",
			CreatedAt: time.Now(),
		}
	})
	return instance
}
