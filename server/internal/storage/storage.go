package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	pulsewatchv1 "pulsewatch/proto/v1"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// HostRecord represents a registered host in the system.
type HostRecord struct {
	HostID        string    `json:"host_id"`
	Hostname      string    `json:"hostname"`
	OS            string    `json:"os"`
	Arch          string    `json:"arch"`
	AgentVersion  string    `json:"agent_version"`
	FirstSeen     time.Time `json:"first_seen"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Status        string    `json:"status"`
}

const (
	// MaxPointsPerBatch caps the number of metrics allowed in a single network batch.
	MaxPointsPerBatch = 2000
	// MaxLabelsPerPoint protects against high-cardinality explosions.
	MaxLabelsPerPoint = 32
)

// MetricRecord represents a query result of a single metric reading.
type MetricRecord struct {
	Timestamp  time.Time         `json:"timestamp"`
	HostID     string            `json:"host_id"`
	MetricName string            `json:"metric_name"`
	Value      float64           `json:"value"`
	Labels     map[string]string `json:"labels"`
}

// Storage manages database operations, interned caching, and idempotency guarantees.
type Storage struct {
	pool        *pgxpool.Pool
	metricCache sync.Map // map[string]int32
	labelCache  sync.Map // map[int64]int64
}

// New creates a new Storage instance connected to TimescaleDB.
func New(ctx context.Context, connString string) (*Storage, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("invalid connection string: %w", err)
	}

	config.MaxConns = 30
	config.MinConns = 5
	config.MaxConnLifetime = 1 * time.Hour
	config.MaxConnIdleTime = 15 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	// Verify connectivity
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("database ping failed: %w", err)
	}

	s := &Storage{
		pool: pool,
	}

	// Preload existing metric definitions into cache
	if err := s.preloadCaches(ctx); err != nil {
		return nil, fmt.Errorf("failed to preload caches: %w", err)
	}

	return s, nil
}

// Close gracefully releases the database connection pool.
func (s *Storage) Close() {
	s.pool.Close()
}

// preloadCaches populates in-memory lookup caches to eliminate database round-trips for existing metrics.
func (s *Storage) preloadCaches(ctx context.Context) error {
	rows, err := s.pool.Query(ctx, "SELECT id, name FROM metric_definitions")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id int32
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return err
		}
		s.metricCache.Store(name, id)
	}
	return rows.Err()
}

// HashLabels computes a deterministic 64-bit FNV hash for sorted key-value pairs.
func HashLabels(labels map[string]string) int64 {
	if len(labels) == 0 {
		return 0
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := fnv.New64a()
	for _, k := range keys {
		_, _ = h.Write([]byte(k))
		_, _ = h.Write([]byte("="))
		_, _ = h.Write([]byte(labels[k]))
		_, _ = h.Write([]byte(";"))
	}
	return int64(h.Sum64())
}

// getOrInsertMetricID resolves an interned metric ID, caching results in memory.
func (s *Storage) getOrInsertMetricID(ctx context.Context, tx pgx.Tx, name string) (int32, error) {
	if val, ok := s.metricCache.Load(name); ok {
		return val.(int32), nil
	}

	var id int32
	query := `
		INSERT INTO metric_definitions (name)
		VALUES ($1)
		ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
		RETURNING id;
	`
	err := tx.QueryRow(ctx, query, name).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to intern metric %q: %w", name, err)
	}

	s.metricCache.Store(name, id)
	return id, nil
}

// getOrInsertLabelSetID resolves an interned label set ID.
func (s *Storage) getOrInsertLabelSetID(ctx context.Context, tx pgx.Tx, labels map[string]string) (int64, error) {
	if labels == nil {
		labels = make(map[string]string)
	}
	hash := HashLabels(labels)
	if val, ok := s.labelCache.Load(hash); ok {
		return val.(int64), nil
	}

	labelsJSON, err := json.Marshal(labels)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal labels: %w", err)
	}

	var id int64
	query := `
		INSERT INTO label_sets (labels_hash, labels)
		VALUES ($1, $2)
		ON CONFLICT (labels_hash) DO UPDATE SET labels_hash = EXCLUDED.labels_hash
		RETURNING id;
	`
	err = tx.QueryRow(ctx, query, hash, labelsJSON).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to intern label set: %w", err)
	}

	s.labelCache.Store(hash, id)
	return id, nil
}

// IngestBatch persists a MetricBatch atomically, enforcing sequence-number idempotency.
// Returns ACK_STATUS_COMMITTED or ACK_STATUS_DUPLICATE.
func (s *Storage) IngestBatch(ctx context.Context, batch *pulsewatchv1.MetricBatch) (pulsewatchv1.AckStatus, error) {
	if len(batch.Points) == 0 {
		return pulsewatchv1.AckStatus_ACK_STATUS_COMMITTED, nil
	}

	// Cardinality limit guard
	if len(batch.Points) > MaxPointsPerBatch {
		return pulsewatchv1.AckStatus_ACK_STATUS_REJECTED, fmt.Errorf("batch exceeds maximum allowed points (%d > %d)", len(batch.Points), MaxPointsPerBatch)
	}

	// 1. Check last acked sequence number for this host
	var lastAckedSeq uint64
	err := s.pool.QueryRow(ctx, "SELECT last_acked_seq FROM agent_sequences WHERE host_id = $1", batch.HostId).
		Scan(&lastAckedSeq)
	if err == nil && batch.Seq <= lastAckedSeq {
		// Batch was already committed in an earlier transaction; idempotent return
		return pulsewatchv1.AckStatus_ACK_STATUS_DUPLICATE, nil
	}

	// 2. Begin atomic transaction
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return pulsewatchv1.AckStatus_ACK_STATUS_RETRYABLE_ERROR, fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 3. Prepare metric points for insertion
	type rowData struct {
		ts         time.Time
		hostID     string
		metricID   int32
		labelSetID int64
		value      float64
	}
	rows := make([]rowData, 0, len(batch.Points))

	for _, pt := range batch.Points {
		metricID, err := s.getOrInsertMetricID(ctx, tx, pt.Name)
		if err != nil {
			return pulsewatchv1.AckStatus_ACK_STATUS_RETRYABLE_ERROR, err
		}

		labelSetID, err := s.getOrInsertLabelSetID(ctx, tx, pt.Labels)
		if err != nil {
			return pulsewatchv1.AckStatus_ACK_STATUS_RETRYABLE_ERROR, err
		}

		rows = append(rows, rowData{
			ts:         time.UnixMilli(pt.TimestampUnixMs),
			hostID:     batch.HostId,
			metricID:   metricID,
			labelSetID: labelSetID,
			value:      pt.Value,
		})
	}

	// 4. Batch insert with ON CONFLICT DO NOTHING
	batchInsert := &pgx.Batch{}
	for _, r := range rows {
		batchInsert.Queue(`
			INSERT INTO metrics (ts, host_id, metric_id, label_set_id, value)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (host_id, metric_id, label_set_id, ts) DO NOTHING;
		`, r.ts, r.hostID, r.metricID, r.labelSetID, r.value)
	}

	br := tx.SendBatch(ctx, batchInsert)
	for range rows {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return pulsewatchv1.AckStatus_ACK_STATUS_RETRYABLE_ERROR, fmt.Errorf("batch insert failed: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return pulsewatchv1.AckStatus_ACK_STATUS_RETRYABLE_ERROR, fmt.Errorf("closing batch results failed: %w", err)
	}

	// 5. Update agent sequence number atomically
	seqQuery := `
		INSERT INTO agent_sequences (host_id, last_acked_seq, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (host_id) DO UPDATE
		SET last_acked_seq = GREATEST(agent_sequences.last_acked_seq, EXCLUDED.last_acked_seq),
		    updated_at = NOW();
	`
	if _, err := tx.Exec(ctx, seqQuery, batch.HostId, batch.Seq); err != nil {
		return pulsewatchv1.AckStatus_ACK_STATUS_RETRYABLE_ERROR, fmt.Errorf("failed to update sequence: %w", err)
	}

	// 6. Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return pulsewatchv1.AckStatus_ACK_STATUS_RETRYABLE_ERROR, fmt.Errorf("commit failed: %w", err)
	}

	return pulsewatchv1.AckStatus_ACK_STATUS_COMMITTED, nil
}

// QueryLatest retrieves the most recent reading for a given host and metric.
func (s *Storage) QueryLatest(ctx context.Context, hostID string, metricName string) (*MetricRecord, error) {
	query := `
		SELECT m.ts, m.host_id, d.name, m.value, l.labels
		FROM metrics m
		JOIN metric_definitions d ON m.metric_id = d.id
		JOIN label_sets l ON m.label_set_id = l.id
		WHERE m.host_id = $1 AND d.name = $2
		ORDER BY m.ts DESC
		LIMIT 1;
	`
	var r MetricRecord
	var labelsJSON []byte
	err := s.pool.QueryRow(ctx, query, strings.TrimSpace(hostID), strings.TrimSpace(metricName)).
		Scan(&r.Timestamp, &r.HostID, &r.MetricName, &r.Value, &labelsJSON)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(labelsJSON, &r.Labels); err != nil {
		r.Labels = make(map[string]string)
	}

	return &r, nil
}

// RecordHeartbeat upserts host liveness information.
func (s *Storage) RecordHeartbeat(ctx context.Context, req *pulsewatchv1.HeartbeatRequest) error {
	query := `
		INSERT INTO hosts (host_id, hostname, os, arch, agent_version, first_seen, last_heartbeat, status)
		VALUES ($1, $2, $3, $4, $5, NOW(), NOW(), 'online')
		ON CONFLICT (host_id) DO UPDATE
		SET hostname = EXCLUDED.hostname,
		    os = EXCLUDED.os,
		    arch = EXCLUDED.arch,
		    agent_version = EXCLUDED.agent_version,
		    last_heartbeat = NOW(),
		    status = 'online';
	`
	_, err := s.pool.Exec(ctx, query, req.HostId, req.Hostname, req.Os, req.Arch, req.AgentVersion)
	return err
}

// ReapInactiveHosts updates hosts whose heartbeat has elapsed past timeout to 'offline'.
func (s *Storage) ReapInactiveHosts(ctx context.Context, timeout time.Duration) (int64, error) {
	query := `
		UPDATE hosts
		SET status = 'offline'
		WHERE status = 'online'
		  AND last_heartbeat < NOW() - ($1 * INTERVAL '1 millisecond');
	`
	tag, err := s.pool.Exec(ctx, query, timeout.Milliseconds())
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ListHosts returns all registered hosts ordered by latest heartbeat.
func (s *Storage) ListHosts(ctx context.Context) ([]HostRecord, error) {
	query := `
		SELECT host_id, hostname, os, arch, agent_version, first_seen, last_heartbeat, status
		FROM hosts
		ORDER BY last_heartbeat DESC;
	`
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hosts []HostRecord
	for rows.Next() {
		var h HostRecord
		if err := rows.Scan(&h.HostID, &h.Hostname, &h.OS, &h.Arch, &h.AgentVersion, &h.FirstSeen, &h.LastHeartbeat, &h.Status); err != nil {
			return nil, err
		}
		hosts = append(hosts, h)
	}
	return hosts, rows.Err()
}
