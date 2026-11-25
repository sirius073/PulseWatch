package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	pulsewatchv1 "pulsewatch/proto/v1"
	"pulsewatch/server/internal/alert"
	"pulsewatch/server/internal/api"
	"pulsewatch/server/internal/buffer"
	"pulsewatch/server/internal/ingest"
	"pulsewatch/server/internal/reaper"
	"pulsewatch/server/internal/storage"
	"pulsewatch/server/internal/telemetry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
	// Accept PORT (Render/Fly/Koyeb standard) or PULSEWATCH_HTTP_PORT; default 8080.
	httpPort := os.Getenv("PORT")
	if httpPort == "" {
		httpPort = os.Getenv("PULSEWATCH_HTTP_PORT")
	}
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

	// 4. Initialize Alert Engine & gRPC Ingest Server
	alertEngine := alert.NewEngine()

	grpcListener, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("[PulseWatch Server] Failed to listen on gRPC port %s: %v", grpcPort, err)
	}

	var grpcOpts []grpc.ServerOption
	if certFile, keyFile := os.Getenv("PULSEWATCH_TLS_CERT"), os.Getenv("PULSEWATCH_TLS_KEY"); certFile != "" && keyFile != "" {
		tlsCreds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
		if err != nil {
			log.Fatalf("[PulseWatch Server] Failed to load gRPC TLS credentials: %v", err)
		}
		grpcOpts = append(grpcOpts, grpc.Creds(tlsCreds))
		log.Println("[PulseWatch Server] gRPC TLS enabled")
	}
	if apiKey := os.Getenv("PULSEWATCH_API_KEY"); apiKey != "" {
		grpcOpts = append(grpcOpts, grpc.UnaryInterceptor(ingest.APIKeyUnaryInterceptor(map[string]struct{}{
			apiKey: {},
		})))
		log.Println("[PulseWatch Server] gRPC API key authentication enabled")
	}

	grpcServer := grpc.NewServer(grpcOpts...)
	ingestService := ingest.NewServer(store, workerQueue, alertEngine)
	pulsewatchv1.RegisterMetricsIngestServiceServer(grpcServer, ingestService)

	go func() {
		log.Printf("[PulseWatch Server] gRPC ingest listening on :%s", grpcPort)
		if err := grpcServer.Serve(grpcListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatalf("[PulseWatch Server] gRPC server error: %v", err)
		}
	}()

	// 5. Start HTTP Query, Health & Telemetry Server
	mux := http.NewServeMux()

	// Health check — both /health (local) and /api/v1/health (Render health check path)
	healthHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":      "ok",
			"queue_depth": workerQueue.Depth(),
		})
	}
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/api/v1/health", healthHandler)

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

	// ── HTTP Ingestion Endpoints (Enables Port 80/443 HTTPS Agents) ────────────
	// POST /api/v1/ingest/batch
	// Accepts metrics over standard HTTP/HTTPS so agents can stream through
	// any corporate firewall or cloud proxy without requiring open port 50051.
	mux.HandleFunc("/api/v1/ingest/batch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			HostID       string `json:"host_id"`
			Seq          uint64 `json:"seq"`
			SentAtUnixMs int64  `json:"sent_at_unix_ms"`
			Points       []struct {
				Name            string            `json:"name"`
				Value           float64           `json:"value"`
				TimestampUnixMs int64             `json:"timestamp_unix_ms"`
				Labels          map[string]string `json:"labels"`
			} `json:"points"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error": "invalid JSON batch payload"}`, http.StatusBadRequest)
			return
		}
		if req.HostID == "" || req.Seq == 0 {
			http.Error(w, `{"error": "host_id and non-zero seq are required"}`, http.StatusBadRequest)
			return
		}

		// Convert to proto MetricBatch to reuse bounded queue with deferred DB commit
		pbBatch := &pulsewatchv1.MetricBatch{
			HostId:       req.HostID,
			Seq:          req.Seq,
			SentAtUnixMs: req.SentAtUnixMs,
			Points:       make([]*pulsewatchv1.MetricPoint, 0, len(req.Points)),
		}

		for _, p := range req.Points {
			pbBatch.Points = append(pbBatch.Points, &pulsewatchv1.MetricPoint{
				Name:            p.Name,
				Value:           p.Value,
				TimestampUnixMs: p.TimestampUnixMs,
				Labels:          p.Labels,
			})
		}

		telemetry.Metrics.IncBatchesReceived()
		telemetry.Metrics.AddPointsReceived(len(pbBatch.Points))

		ackStatus, err := workerQueue.EnqueueAndWait(r.Context(), pbBatch)
		if err != nil {
			telemetry.Metrics.IncCommitErrors()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"host_id": req.HostID,
				"seq":     req.Seq,
				"status":  "RETRYABLE_ERROR",
				"error":   err.Error(),
			})
			return
		}

		statusStr := "COMMITTED"
		if ackStatus == pulsewatchv1.AckStatus_ACK_STATUS_COMMITTED {
			telemetry.Metrics.IncBatchesCommitted()
			for _, pt := range pbBatch.Points {
				var ts time.Time
				if pt.TimestampUnixMs > 0 {
					ts = time.UnixMilli(pt.TimestampUnixMs)
				} else {
					ts = time.Now()
				}
				alertEngine.EvaluateMetric(req.HostID, pt.Name, pt.Value, ts)
			}
		} else if ackStatus == pulsewatchv1.AckStatus_ACK_STATUS_DUPLICATE {
			telemetry.Metrics.IncBatchesDuplicate()
			statusStr = "DUPLICATE"
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"host_id": req.HostID,
			"seq":     req.Seq,
			"status":  statusStr,
		})
	})

	// POST /api/v1/ingest/heartbeat
	mux.HandleFunc("/api/v1/ingest/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			HostID       string `json:"host_id"`
			Hostname     string `json:"hostname"`
			OS           string `json:"os"`
			Arch         string `json:"arch"`
			AgentVersion string `json:"agent_version"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error": "invalid JSON heartbeat payload"}`, http.StatusBadRequest)
			return
		}
		if req.HostID == "" {
			http.Error(w, `{"error": "host_id is required"}`, http.StatusBadRequest)
			return
		}

		hb := &pulsewatchv1.HeartbeatRequest{
			HostId:       req.HostID,
			Hostname:     req.Hostname,
			Os:           req.OS,
			Arch:         req.Arch,
			AgentVersion: req.AgentVersion,
		}

		if err := store.RecordHeartbeat(r.Context(), hb); err != nil {
			http.Error(w, fmt.Sprintf(`{"error": %q}`, err.Error()), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"acknowledged":        true,
			"server_time_unix_ms": time.Now().UnixMilli(),
		})
	})

	// ── Range query endpoint ───────────────────────────────────────────────────
	// GET /api/v1/metrics/range?host=&metric=&from=&to=&step=&agg=
	//
	// from/to: RFC3339 timestamps (optional — defaults to last 1h)
	// step:    1m | 5m | 1h  (optional — auto-selected based on window)
	// agg:     avg | min | max | sum  (optional — default avg)
	//
	// The handler auto-selects raw vs rollup table based on window size.
	mux.HandleFunc("/api/v1/metrics/range", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()

		p := api.QueryRangeParams{
			HostID:     q.Get("host"),
			MetricName: q.Get("metric"),
			Step:       q.Get("step"),
			Agg:        q.Get("agg"),
		}

		if fromStr := q.Get("from"); fromStr != "" {
			t, err := time.Parse(time.RFC3339, fromStr)
			if err != nil {
				http.Error(w, `{"error": "invalid 'from' timestamp, use RFC3339"}`, http.StatusBadRequest)
				return
			}
			p.From = t
		}
		if toStr := q.Get("to"); toStr != "" {
			t, err := time.Parse(time.RFC3339, toStr)
			if err != nil {
				http.Error(w, `{"error": "invalid 'to' timestamp, use RFC3339"}`, http.StatusBadRequest)
				return
			}
			p.To = t
		}

		if err := api.ValidateRangeParams(&p); err != nil {
			http.Error(w, fmt.Sprintf(`{"error": %q}`, err.Error()), http.StatusBadRequest)
			return
		}

		// Convert api.QueryRangeParams → storage.QueryRangeParams.
		// The table selection lives in the api package so storage stays DB-only.
		sp := storage.QueryRangeParams{
			HostID:     p.HostID,
			MetricName: p.MetricName,
			From:       p.From,
			To:         p.To,
			Table:      api.TableFor(p.From, p.To),
			Step:       p.Step,
			Agg:        p.Agg,
		}

		pts, err := store.QueryRange(r.Context(), sp)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error": %q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		if pts == nil {
			pts = []storage.DataPoint{} // never return null to the client
		}

		result := struct {
			HostID     string              `json:"host_id"`
			MetricName string              `json:"metric_name"`
			From       time.Time           `json:"from"`
			To         time.Time           `json:"to"`
			Step       string              `json:"step"`
			Agg        string              `json:"agg"`
			Table      string              `json:"table"`
			Points     []storage.DataPoint `json:"points"`
		}{
			HostID:     p.HostID,
			MetricName: p.MetricName,
			From:       p.From,
			To:         p.To,
			Step:       p.Step,
			Agg:        p.Agg,
			Table:      sp.Table,
			Points:     pts,
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	})

	// ── SSE streaming endpoint ─────────────────────────────────────────────────
	// GET /api/v1/stream/metrics?host=&metric=&interval=5
	//
	// Pushes Server-Sent Events with the latest metric value every `interval`
	// seconds (default 5). SSE is simpler than WebSocket for one-way streaming:
	// the browser's EventSource API handles reconnection automatically.
	//
	// Each event:   data: {"ts":"...","value":42.1}\n\n
	mux.HandleFunc("/api/v1/stream/metrics", func(w http.ResponseWriter, r *http.Request) {
		hostID := r.URL.Query().Get("host")
		metricName := r.URL.Query().Get("metric")
		if hostID == "" || metricName == "" {
			http.Error(w, `{"error": "host and metric are required"}`, http.StatusBadRequest)
			return
		}
		intervalSec := 5
		if iv := r.URL.Query().Get("interval"); iv != "" {
			if n, err := strconv.Atoi(iv); err == nil && n >= 1 && n <= 60 {
				intervalSec = n
			}
		}

		// SSE headers — no buffering, keep-alive, CORS for the frontend dev server.
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		ticker := time.NewTicker(time.Duration(intervalSec) * time.Second)
		defer ticker.Stop()

		// Send one event immediately so the client doesn't wait for the first tick.
		sendSSEEvent := func() {
			val, ts, err := store.QueryLatestValue(r.Context(), hostID, metricName)
			if err != nil {
				// Metric not found yet — send a keepalive comment instead.
				fmt.Fprintf(w, ": waiting for data\n\n")
				flusher.Flush()
				return
			}
			payload, _ := json.Marshal(map[string]interface{}{
				"ts":    ts.Format(time.RFC3339),
				"value": val,
			})
			fmt.Fprintf(w, "data: %s\n\n", payload)
			flusher.Flush()
		}

		sendSSEEvent()

		for {
			select {
			case <-r.Context().Done():
				return // client disconnected
			case <-ticker.C:
				sendSSEEvent()
			}
		}
	})

	// ── Alert API endpoints ──────────────────────────────────────────────────
	// GET /api/v1/rules - List configured rules
	// POST /api/v1/rules - Create or update rule
	// DELETE /api/v1/rules?id= - Delete rule
	mux.HandleFunc("/api/v1/rules", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(alertEngine.ListRules())
		case http.MethodPost:
			var req struct {
				ID          string  `json:"id"`
				Name        string  `json:"name"`
				HostID      string  `json:"host_id"`
				MetricName  string  `json:"metric_name"`
				Condition   string  `json:"condition"`
				Threshold   float64 `json:"threshold"`
				Detector    string  `json:"detector"`
				DurationSec int     `json:"duration_sec"`
				Severity    string  `json:"severity"`
				WebhookURL  string  `json:"webhook_url"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, `{"error": "invalid JSON request"}`, http.StatusBadRequest)
				return
			}
			rule := alert.Rule{
				ID:         req.ID,
				Name:       req.Name,
				HostID:     req.HostID,
				MetricName: req.MetricName,
				Condition:  req.Condition,
				Threshold:  req.Threshold,
				Detector:   req.Detector,
				Duration:   time.Duration(req.DurationSec) * time.Second,
				Severity:   req.Severity,
				WebhookURL: req.WebhookURL,
			}
			if err := alertEngine.AddRule(rule); err != nil {
				http.Error(w, fmt.Sprintf(`{"error": %q}`, err.Error()), http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "rule": rule})
		case http.MethodDelete:
			id := r.URL.Query().Get("id")
			if id == "" {
				http.Error(w, `{"error": "id parameter required"}`, http.StatusBadRequest)
				return
			}
			deleted := alertEngine.DeleteRule(id)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "deleted": deleted})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// GET /api/v1/alerts - List active and historical alerts
	mux.HandleFunc("/api/v1/alerts", func(w http.ResponseWriter, r *http.Request) {
		filter := r.URL.Query().Get("state")
		w.Header().Set("Content-Type", "application/json")
		switch filter {
		case "active", "firing", "pending":
			_ = json.NewEncoder(w).Encode(alertEngine.ListActiveAlerts())
		case "history", "resolved":
			_ = json.NewEncoder(w).Encode(alertEngine.ListAlertHistory())
		default:
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"active":        alertEngine.ListActiveAlerts(),
				"history":       alertEngine.ListAlertHistory(),
				"notifications": alertEngine.ListNotifications(),
			})
		}
	})

	// ── Proxy to Python ML Predictive Sidecar ────────────────────────────────
	sidecarURL := os.Getenv("PULSEWATCH_SIDECAR_URL")
	if sidecarURL == "" {
		sidecarURL = "http://localhost:8000"
	}
	if target, err := url.Parse(sidecarURL); err == nil {
		proxy := httputil.NewSingleHostReverseProxy(target)
		mux.HandleFunc("/api/v1/predict/", func(w http.ResponseWriter, r *http.Request) {
			r.URL.Path = strings.TrimPrefix(r.URL.Path, "/api/v1")
			proxy.ServeHTTP(w, r)
		})
	}

	// ── Static SPA frontend serving ──────────────────────────────────────────
	if _, err := os.Stat("frontend/dist"); err == nil {
		fileServer := http.FileServer(http.Dir("frontend/dist"))
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			cleanPath := filepath.Clean(r.URL.Path)
			diskPath := filepath.Join("frontend/dist", cleanPath)
			if fi, err := os.Stat(diskPath); os.IsNotExist(err) || fi.IsDir() {
				// Fallback to index.html for client-side routing
				http.ServeFile(w, r, "frontend/dist/index.html")
				return
			}
			fileServer.ServeHTTP(w, r)
		})
		log.Println("[PulseWatch Server] Serving frontend UI from frontend/dist")
	}

	// Wrap in CORS handler
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		mux.ServeHTTP(w, r)
	})

	httpServer := &http.Server{
		Addr:        ":" + httpPort,
		Handler:     handler,
		ReadTimeout: 5 * time.Second,
		// WriteTimeout must be 0 (no timeout) because SSE connections are
		// long-lived. A non-zero value would kill the stream after the timeout.
		WriteTimeout: 0,
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
