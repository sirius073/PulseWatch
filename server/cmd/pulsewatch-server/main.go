package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pulsewatchv1 "pulsewatch/proto/v1"
	"pulsewatch/server/internal/buffer"
	"pulsewatch/server/internal/ingest"
	"pulsewatch/server/internal/reaper"
	"pulsewatch/server/internal/storage"
	"pulsewatch/server/internal/telemetry"

	"google.golang.org/grpc"
)

func main() {
	log.Println("[PulseWatch Server] Starting...")

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://pulsewatch:pulsewatch_secret@localhost:5432/pulsewatch?sslmode=disable"
	}
	grpcPort := os.Getenv("PULSEWATCH_GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50051"
	}
	httpPort := os.Getenv("PULSEWATCH_HTTP_PORT")
	if httpPort == "" {
		httpPort = "8080"
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize Storage Engine
	store, err := storage.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("[PulseWatch Server] Failed to initialize storage: %v", err)
	}
	defer store.Close()
	log.Println("[PulseWatch Server] Connected to TimescaleDB")

	// 2. Initialize Bounded Queue & DB Worker Pool
	workerQueue := buffer.NewQueue(store, 5000, 8)
	defer workerQueue.Close()

	// 3. Start Inactive Host Reaper
	hostReaper := reaper.New(store, 10*time.Second, 30*time.Second)
	hostReaper.Start()
	defer hostReaper.Stop()

	// 4. Start gRPC Ingest Server
	grpcListener, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("[PulseWatch Server] Failed to listen on gRPC port %s: %v", grpcPort, err)
	}
	grpcServer := grpc.NewServer()
	ingestService := ingest.NewServer(store, workerQueue)
	pulsewatchv1.RegisterMetricsIngestServiceServer(grpcServer, ingestService)

	go func() {
		log.Printf("[PulseWatch Server] gRPC ingest listening on :%s", grpcPort)
		if err := grpcServer.Serve(grpcListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatalf("[PulseWatch Server] gRPC server error: %v", err)
		}
	}()

	// 5. Start HTTP Query, Health & Telemetry Server
	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":      "ok",
			"queue_depth": workerQueue.Depth(),
		})
	})

	// Prometheus Self-Monitoring metrics
	activeHostsCounter := func() int {
		hosts, err := store.ListHosts(context.Background())
		if err != nil {
			return 0
		}
		active := 0
		for _, h := range hosts {
			if h.Status == "online" {
				active++
			}
		}
		return active
	}
	mux.HandleFunc("/metrics", telemetry.Metrics.Handler(workerQueue.Depth, activeHostsCounter))

	// Hosts inventory endpoint
	mux.HandleFunc("/api/v1/hosts", func(w http.ResponseWriter, r *http.Request) {
		hosts, err := store.ListHosts(r.Context())
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error": "%s"}`, err.Error()), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(hosts)
	})

	// Metric latest reading endpoint
	mux.HandleFunc("/api/v1/metrics/latest", func(w http.ResponseWriter, r *http.Request) {
		hostID := r.URL.Query().Get("host")
		metricName := r.URL.Query().Get("metric")
		if hostID == "" || metricName == "" {
			http.Error(w, `{"error": "host and metric query parameters are required"}`, http.StatusBadRequest)
			return
		}

		record, err := store.QueryLatest(r.Context(), hostID, metricName)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error": "%s"}`, err.Error()), http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(record)
	})

	httpServer := &http.Server{
		Addr:         ":" + httpPort,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("[PulseWatch Server] HTTP API & /metrics listening on :%s", httpPort)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[PulseWatch Server] HTTP server error: %v", err)
		}
	}()

	// 6. Graceful Shutdown on SIGINT / SIGTERM
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("[PulseWatch Server] Shutting down gracefully...")
	grpcServer.GracefulStop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)

	log.Println("[PulseWatch Server] Stopped.")
}
