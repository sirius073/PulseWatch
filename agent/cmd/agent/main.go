package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"pulsewatch/agent/internal/collector"
	"pulsewatch/agent/internal/sender"
	"pulsewatch/agent/internal/wal"
	pulsewatchv1 "pulsewatch/proto/v1"
)

const agentVersion = "v1.0.0"

func main() {
	log.Println("[PulseWatch Agent] Starting with full collector suite & heartbeat...")

	serverAddr := os.Getenv("PULSEWATCH_SERVER_ADDR")
	if serverAddr == "" {
		serverAddr = "localhost:50051"
	}

	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "pulsewatch-agent"
	}

	hostID := os.Getenv("PULSEWATCH_HOST_ID")
	if hostID == "" {
		hostID = hostname
	}

	walDir := os.Getenv("PULSEWATCH_WAL_DIR")
	if walDir == "" {
		walDir = "wal_data"
	}

	collectInterval := 3 * time.Second
	heartbeatInterval := 10 * time.Second

	// 1. Initialize Local Disk WAL
	diskWAL, err := wal.Open(wal.Config{
		Dir:               walDir,
		MaxSegmentBytes:   5 * 1024 * 1024,  // 5 MB segments
		MaxTotalDiskBytes: 50 * 1024 * 1024, // 50 MB max storage
	})
	if err != nil {
		log.Fatalf("[PulseWatch Agent] Failed to initialize WAL: %v", err)
	}
	defer diskWAL.Close()

	// 2. Initialize Sender & Backoff
	snd, err := sender.New(serverAddr, hostID)
	if err != nil {
		log.Fatalf("[PulseWatch Agent] Failed to initialize sender: %v", err)
	}
	defer snd.Close()

	backoff := sender.NewBackoff(500*time.Millisecond, 15*time.Second)

	// 3. Initialize Full Suite of Collectors
	collectors := []collector.Collector{
		collector.NewCPUCollector(100 * time.Millisecond),
		collector.NewMemoryCollector(),
		collector.NewDiskCollector(),
		collector.NewNetworkCollector(),
		collector.NewProcessCollector(10), // Strictly Top-10 by resource usage
	}

	log.Printf("[PulseWatch Agent] Host ID: %s | OS: %s/%s | Collectors: %d", hostID, runtime.GOOS, runtime.GOARCH, len(collectors))

	// Send initial heartbeat immediately
	initHbCtx, initHbCancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, _ = snd.SendHeartbeat(initHbCtx, hostname, runtime.GOOS, runtime.GOARCH, agentVersion)
	initHbCancel()

	// Tickers
	collectTicker := time.NewTicker(collectInterval)
	defer collectTicker.Stop()

	heartbeatTicker := time.NewTicker(heartbeatInterval)
	defer heartbeatTicker.Stop()

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	for {
		select {
		case <-stopChan:
			log.Println("[PulseWatch Agent] Received shutdown signal, exiting...")
			return

		case <-heartbeatTicker.C:
			// Send host liveness heartbeat
			hbCtx, hbCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, hbErr := snd.SendHeartbeat(hbCtx, hostname, runtime.GOOS, runtime.GOARCH, agentVersion)
			hbCancel()
			if hbErr != nil {
				log.Printf("[Agent Heartbeat] Failed to emit heartbeat: %v", hbErr)
			}

		case <-collectTicker.C:
			// Step A: Attempt to drain and replay WAL if there is buffered data
			if diskWAL.HasBufferedData() {
				replayBatches, err := diskWAL.ReplayAll()
				if err == nil && len(replayBatches) > 0 {
					log.Printf("[Agent WAL] Found %d buffered batches to replay...", len(replayBatches))
					allReplayed := true
					for _, b := range replayBatches {
						replayCtx, replayCancel := context.WithTimeout(context.Background(), 3*time.Second)
						ack, err := snd.SendPreparedBatch(replayCtx, b)
						replayCancel()

						if err != nil || (ack != nil && ack.Status == pulsewatchv1.AckStatus_ACK_STATUS_RETRYABLE_ERROR) {
							log.Printf("[Agent WAL] Replay failed at seq=%d, will retry with backoff: %v", b.Seq, err)
							allReplayed = false
							break
						}
					}

					if allReplayed {
						log.Printf("[Agent WAL] Successfully replayed all %d batches! Purging disk WAL.", len(replayBatches))
						_ = diskWAL.PurgeAll()
						backoff.Reset()
					}
				}
			}

			// Step B: Collect metrics from all active collectors
			var allPoints []*pulsewatchv1.MetricPoint
			for _, col := range collectors {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				points, err := col.Collect(ctx)
				cancel()

				if err != nil {
					log.Printf("[PulseWatch Agent] Collector %s error: %v", col.Name(), err)
					continue
				}
				allPoints = append(allPoints, points...)
			}

			// Step C: Send or Spool to WAL
			if diskWAL.HasBufferedData() {
				// Server currently offline; spool directly to disk to preserve sequence
				batch := &pulsewatchv1.MetricBatch{
					HostId:       hostID,
					Seq:          snd.NextSeq(),
					SentAtUnixMs: time.Now().UnixMilli(),
					Points:       allPoints,
				}
				if err := diskWAL.Append(batch); err != nil {
					log.Printf("[Agent WAL] Critical error spooling to WAL: %v", err)
				} else {
					log.Printf("[Agent WAL] Spooled batch seq=%d (%d points) to disk", batch.Seq, len(allPoints))
				}
			} else {
				// Normal dispatch
				sendCtx, sendCancel := context.WithTimeout(context.Background(), 3*time.Second)
				ack, batch, err := snd.SendBatch(sendCtx, allPoints)
				sendCancel()

				if err != nil || (ack != nil && ack.Status == pulsewatchv1.AckStatus_ACK_STATUS_RETRYABLE_ERROR) {
					if batch != nil {
						if walErr := diskWAL.Append(batch); walErr != nil {
							log.Printf("[Agent WAL] Error spooling to WAL: %v", walErr)
						} else {
							log.Printf("[Agent WAL] Server unreachable; spooled batch seq=%d (%d points) to disk", batch.Seq, len(allPoints))
						}
					}
				} else {
					backoff.Reset()
				}
			}
		}
	}
}
