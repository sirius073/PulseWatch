package reaper

import (
	"context"
	"log"
	"time"

	"pulsewatch/server/internal/storage"
)

// Reaper periodically scans the hosts table and marks inactive hosts as 'offline'.
type Reaper struct {
	store    *storage.Storage
	interval time.Duration
	timeout  time.Duration
	stopChan chan struct{}
}

// New constructs a new host reaper.
// interval: how often the reaper runs (e.g. 15s).
// timeout: elapsed time without heartbeat before marking offline (e.g. 30s).
func New(store *storage.Storage, interval, timeout time.Duration) *Reaper {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Reaper{
		store:    store,
		interval: interval,
		timeout:  timeout,
		stopChan: make(chan struct{}),
	}
}

// Start begins the background reaper loop.
func (r *Reaper) Start() {
	go func() {
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()

		for {
			select {
			case <-r.stopChan:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				count, err := r.store.ReapInactiveHosts(ctx, r.timeout)
				cancel()

				if err != nil {
					log.Printf("[Reaper] Error reaping inactive hosts: %v", err)
				} else if count > 0 {
					log.Printf("[Reaper] Marked %d inactive hosts as 'offline'", count)
				}
			}
		}
	}()
}

// Stop terminates the background reaper loop.
func (r *Reaper) Stop() {
	close(r.stopChan)
}
