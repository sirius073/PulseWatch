// Package main implements the PulseWatch distributed load generator.
// It simulates N concurrent host agents transmitting metric batches over gRPC
// to stress-test ingest throughput, deferred ack latencies, and buffer handling.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	pulsewatchv1 "pulsewatch/proto/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type LatencyTracker struct {
	mu      sync.Mutex
	samples []time.Duration
}

func (l *LatencyTracker) Record(d time.Duration) {
	l.mu.Lock()
	l.samples = append(l.samples, d)
	l.mu.Unlock()
}

func (l *LatencyTracker) Percentiles() (p50, p90, p95, p99, min, max time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	n := len(l.samples)
	if n == 0 {
		return 0, 0, 0, 0, 0, 0
	}

	sorted := make([]time.Duration, n)
	copy(sorted, l.samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	min = sorted[0]
	max = sorted[n-1]
	p50 = sorted[int(float64(n)*0.50)]
	p90 = sorted[int(float64(n)*0.90)]
	p95 = sorted[int(float64(n)*0.95)]
	p99 = sorted[int(float64(n)*0.99)]
	return
}

func main() {
	serverAddr := flag.String("server", "localhost:50051", "PulseWatch gRPC server address")
	numAgents := flag.Int("agents", 50, "Number of concurrent simulated agents")
	pointsPerBatch := flag.Int("points-per-batch", 20, "Number of metric points per batch")
	interval := flag.Duration("interval", 1*time.Second, "Batch send interval per agent")
	duration := flag.Duration("duration", 30*time.Second, "Test run duration")
	flag.Parse()

	log.Printf("=========================================================")
	log.Printf("  PulseWatch Distributed Load Generator Benchmark")
	log.Printf("  Target Server    : %s", *serverAddr)
	log.Printf("  Simulated Agents : %d", *numAgents)
	log.Printf("  Points / Batch   : %d", *pointsPerBatch)
	log.Printf("  Batch Interval   : %s", *interval)
	log.Printf("  Test Duration    : %s", *duration)
	log.Printf("=========================================================")

	// Establish gRPC connection pool (shared channel with HTTP/2 multiplexing)
	conn, err := grpc.NewClient(
		*serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server %s: %v", *serverAddr, err)
	}
	defer conn.Close()

	client := pulsewatchv1.NewMetricsIngestServiceClient(conn)

	// Context for benchmark duration
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var (
		totalBatchesSent uint64
		totalPointsSent  uint64
		totalCommitted   uint64
		totalErrors      uint64
	)

	tracker := &LatencyTracker{samples: make([]time.Duration, 0, 10000)}

	startTime := time.Now()
	var wg sync.WaitGroup

	// Progress reporter
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				elapsed := time.Since(startTime).Seconds()
				batches := atomic.LoadUint64(&totalBatchesSent)
				points := atomic.LoadUint64(&totalPointsSent)
				errs := atomic.LoadUint64(&totalErrors)
				rate := float64(points) / elapsed
				log.Printf("[Progress] %4.1fs | Points: %-8d | Batches: %-6d | Rate: %7.1f pts/sec | Errs: %d",
					elapsed, points, batches, rate, errs)
			}
		}
	}()

	// Launch N concurrent agent workers
	for i := 0; i < *numAgents; i++ {
		wg.Add(1)
		agentID := fmt.Sprintf("sim-agent-%04d", i)

		go func(hostID string) {
			defer wg.Done()

			var seq uint64 = 1
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			ticker := time.NewTicker(*interval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					// Build synthetic metric points
					points := make([]*pulsewatchv1.MetricPoint, 0, *pointsPerBatch)
					nowMs := time.Now().UnixMilli()

					// CPU total & cores
					points = append(points, &pulsewatchv1.MetricPoint{
						Name:             "system.cpu.total",
						Value:            10.0 + r.Float64()*50.0,
						TimestampUnixMs: nowMs,
						Labels:           map[string]string{"type": "total"},
					})
					points = append(points, &pulsewatchv1.MetricPoint{
						Name:             "system.memory.used_percent",
						Value:            40.0 + r.Float64()*30.0,
						TimestampUnixMs: nowMs,
						Labels:           map[string]string{"type": "ram"},
					})
					points = append(points, &pulsewatchv1.MetricPoint{
						Name:             "system.disk.used_percent",
						Value:            65.0 + r.Float64()*5.0,
						TimestampUnixMs: nowMs,
						Labels:           map[string]string{"mount": "/"},
					})
					points = append(points, &pulsewatchv1.MetricPoint{
						Name:             "system.network.bytes_sent",
						Value:            1000.0 + r.Float64()*50000.0,
						TimestampUnixMs: nowMs,
						Labels:           map[string]string{"interface": "eth0"},
					})

					// Additional metric points up to requested batch size
					for len(points) < *pointsPerBatch {
						idx := len(points)
						points = append(points, &pulsewatchv1.MetricPoint{
							Name:             fmt.Sprintf("custom.metric.%02d", idx),
							Value:            r.Float64() * 100.0,
							TimestampUnixMs: nowMs,
							Labels:           map[string]string{"dim": "synthetic"},
						})
					}

					batch := &pulsewatchv1.MetricBatch{
						HostId:        hostID,
						Seq:           seq,
						SentAtUnixMs: nowMs,
						Points:        points,
					}
					seq++

					// Measure end-to-end gRPC deferred ack round-trip
					t0 := time.Now()
					reqCtx, reqCancel := context.WithTimeout(context.Background(), 5*time.Second)
					ack, err := client.IngestBatch(reqCtx, batch)
					latency := time.Since(t0)
					reqCancel()

					atomic.AddUint64(&totalBatchesSent, 1)
					atomic.AddUint64(&totalPointsSent, uint64(len(points)))

					if err != nil {
						atomic.AddUint64(&totalErrors, 1)
					} else {
						tracker.Record(latency)
						if ack.Status == pulsewatchv1.AckStatus_ACK_STATUS_COMMITTED {
							atomic.AddUint64(&totalCommitted, 1)
						}
					}
				}
			}
		}(agentID)
	}

	wg.Wait()
	totalElapsed := time.Since(startTime)

	// Output final benchmark report
	batches := atomic.LoadUint64(&totalBatchesSent)
	points := atomic.LoadUint64(&totalPointsSent)
	committed := atomic.LoadUint64(&totalCommitted)
	errors := atomic.LoadUint64(&totalErrors)
	ptsPerSec := float64(points) / totalElapsed.Seconds()
	batchesPerSec := float64(batches) / totalElapsed.Seconds()

	p50, p90, p95, p99, minLat, maxLat := tracker.Percentiles()

	fmt.Println()
	fmt.Println("=========================================================")
	fmt.Println("             PULSEWATCH BENCHMARK REPORT                 ")
	fmt.Println("=========================================================")
	fmt.Printf("Total Duration    : %.2f seconds\n", totalElapsed.Seconds())
	fmt.Printf("Simulated Agents  : %d\n", *numAgents)
	fmt.Printf("Batches Sent      : %d (%.1f batches/sec)\n", batches, batchesPerSec)
	fmt.Printf("Points Sent       : %d (%.1f points/sec)\n", points, ptsPerSec)
	fmt.Printf("Batches Committed : %d\n", committed)
	fmt.Printf("Total Errors      : %d (%.2f%% error rate)\n", errors, float64(errors)/math.Max(1, float64(batches))*100.0)
	fmt.Println("---------------------------------------------------------")
	fmt.Println("Round-Trip Ingest Latency (Deferred Ack Guarantee):")
	fmt.Printf("  Min  : %v\n", minLat)
	fmt.Printf("  p50  : %v\n", p50)
	fmt.Printf("  p90  : %v\n", p90)
	fmt.Printf("  p95  : %v\n", p95)
	fmt.Printf("  p99  : %v\n", p99)
	fmt.Printf("  Max  : %v\n", maxLat)
	fmt.Println("=========================================================")
}
