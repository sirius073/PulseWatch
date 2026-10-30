package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	pulsewatchv1 "pulsewatch/proto/v1"

	"google.golang.org/protobuf/proto"
)

var (
	// ErrCorruptRecord is returned when a record fails CRC32 verification.
	ErrCorruptRecord = errors.New("wal: corrupt record detected (crc32 mismatch)")
	// ErrNoMoreRecords indicates all buffered records have been replayed.
	ErrNoMoreRecords = errors.New("wal: no more records to replay")
)

const (
	// Record layout: [4 bytes payload length] [4 bytes CRC32] [N bytes protobuf payload]
	headerLength = 8
	magicPrefix  = "segment_"
	magicSuffix  = ".wal"
)

// Config configures WAL storage limits and behavior.
type Config struct {
	Dir               string
	MaxSegmentBytes   int64 // Maximum size of a single segment before rolling (e.g. 5MB)
	MaxTotalDiskBytes int64 // Bounded total disk size (e.g. 50MB); oldest segments purged if exceeded
}

// WAL provides a durable, segmented, disk-backed write-ahead log for metric batches.
type WAL struct {
	mu           sync.Mutex
	cfg          Config
	activeFile   *os.File
	activeSegID  uint64
	activeSize   int64
	segments     []uint64 // sorted segment IDs currently on disk
	totalDiskUse int64
}

// Open initializes or recovers the WAL from the specified directory.
func Open(cfg Config) (*WAL, error) {
	if cfg.MaxSegmentBytes <= 0 {
		cfg.MaxSegmentBytes = 5 * 1024 * 1024 // 5 MB default
	}
	if cfg.MaxTotalDiskBytes <= 0 {
		cfg.MaxTotalDiskBytes = 50 * 1024 * 1024 // 50 MB default
	}

	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create wal dir: %w", err)
	}

	w := &WAL{
		cfg: cfg,
	}

	if err := w.scanSegments(); err != nil {
		return nil, err
	}

	// If no segments exist, start segment 1
	if len(w.segments) == 0 {
		if err := w.rollSegment(1); err != nil {
			return nil, err
		}
	} else {
		// Open the last segment for appending
		lastSeg := w.segments[len(w.segments)-1]
		if err := w.openActiveSegment(lastSeg); err != nil {
			return nil, err
		}
	}

	return w, nil
}

// segmentPath returns the file path for a given segment ID.
func (w *WAL) segmentPath(id uint64) string {
	return filepath.Join(w.cfg.Dir, fmt.Sprintf("%s%010d%s", magicPrefix, id, magicSuffix))
}

// scanSegments scans the directory for existing segment files.
func (w *WAL) scanSegments() error {
	entries, err := os.ReadDir(w.cfg.Dir)
	if err != nil {
		return err
	}

	var segs []uint64
	var totalSize int64

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, magicPrefix) && strings.HasSuffix(name, magicSuffix) {
			numPart := strings.TrimSuffix(strings.TrimPrefix(name, magicPrefix), magicSuffix)
			id, err := strconv.ParseUint(numPart, 10, 64)
			if err == nil {
				segs = append(segs, id)
				info, err := entry.Info()
				if err == nil {
					totalSize += info.Size()
				}
			}
		}
	}

	sort.Slice(segs, func(i, j int) bool {
		return segs[i] < segs[j]
	})

	w.segments = segs
	w.totalDiskUse = totalSize
	return nil
}

// openActiveSegment opens an existing segment for appending.
func (w *WAL) openActiveSegment(id uint64) error {
	path := w.segmentPath(id)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open segment %d: %w", id, err)
	}

	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return err
	}

	w.activeFile = file
	w.activeSegID = id
	w.activeSize = stat.Size()
	return nil
}

// rollSegment closes current active segment and creates the next one.
func (w *WAL) rollSegment(newID uint64) error {
	if w.activeFile != nil {
		_ = w.activeFile.Sync()
		_ = w.activeFile.Close()
		w.activeFile = nil
	}

	path := w.segmentPath(newID)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create segment %d: %w", newID, err)
	}

	w.activeFile = file
	w.activeSegID = newID
	w.activeSize = 0

	// Track new segment
	if len(w.segments) == 0 || w.segments[len(w.segments)-1] != newID {
		w.segments = append(w.segments, newID)
	}

	// Enforce bounded disk size by purging oldest segments if needed
	w.pruneOldSegments()
	return nil
}

// pruneOldSegments enforces the MaxTotalDiskBytes bound by deleting oldest segments.
func (w *WAL) pruneOldSegments() {
	for w.totalDiskUse > w.cfg.MaxTotalDiskBytes && len(w.segments) > 1 {
		oldestID := w.segments[0]
		path := w.segmentPath(oldestID)
		info, err := os.Stat(path)
		if err == nil {
			w.totalDiskUse -= info.Size()
		}
		_ = os.Remove(path)
		w.segments = w.segments[1:]
	}
}

// Append persists a MetricBatch to the active WAL segment.
func (w *WAL) Append(batch *pulsewatchv1.MetricBatch) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	payload, err := proto.Marshal(batch)
	if err != nil {
		return fmt.Errorf("failed to marshal batch for wal: %w", err)
	}

	payloadLen := uint32(len(payload))
	checksum := crc32.ChecksumIEEE(payload)

	var header [headerLength]byte
	binary.BigEndian.PutUint32(header[0:4], payloadLen)
	binary.BigEndian.PutUint32(header[4:8], checksum)

	recordBytes := int64(headerLength + len(payload))

	// Check if rolling to a new segment is required
	if w.activeSize+recordBytes > w.cfg.MaxSegmentBytes {
		if err := w.rollSegment(w.activeSegID + 1); err != nil {
			return err
		}
	}

	// Write header + payload
	if _, err := w.activeFile.Write(header[:]); err != nil {
		return fmt.Errorf("failed to write wal header: %w", err)
	}
	if _, err := w.activeFile.Write(payload); err != nil {
		return fmt.Errorf("failed to write wal payload: %w", err)
	}

	// Sync to ensure durability
	if err := w.activeFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync wal: %w", err)
	}

	w.activeSize += recordBytes
	w.totalDiskUse += recordBytes
	return nil
}

// ReplayAll reads all stored batches across all segments in sequence.
// Used when recovering connection to replay buffered metrics.
func (w *WAL) ReplayAll() ([]*pulsewatchv1.MetricBatch, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.activeFile != nil {
		_ = w.activeFile.Sync()
	}

	var allBatches []*pulsewatchv1.MetricBatch

	for _, segID := range w.segments {
		path := w.segmentPath(segID)
		batches, err := readSegment(path)
		if err != nil {
			return nil, fmt.Errorf("error reading segment %d: %w", segID, err)
		}
		allBatches = append(allBatches, batches...)
	}

	return allBatches, nil
}

// PurgeAll removes all segments and resets the WAL to an empty segment 1.
// Called after all buffered batches have been successfully replayed and acknowledged.
func (w *WAL) PurgeAll() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.activeFile != nil {
		_ = w.activeFile.Close()
		w.activeFile = nil
	}

	for _, segID := range w.segments {
		_ = os.Remove(w.segmentPath(segID))
	}

	w.segments = nil
	w.totalDiskUse = 0
	return w.rollSegment(1)
}

// HasBufferedData checks if there are unpurged records in the WAL.
func (w *WAL) HasBufferedData() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.totalDiskUse > 0
}

// Close gracefully flushes and closes the active WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.activeFile != nil {
		_ = w.activeFile.Sync()
		err := w.activeFile.Close()
		w.activeFile = nil
		return err
	}
	return nil
}

// readSegment reads and validates all MetricBatch records from a segment file.
func readSegment(path string) ([]*pulsewatchv1.MetricBatch, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var batches []*pulsewatchv1.MetricBatch
	var header [headerLength]byte

	for {
		_, err := io.ReadFull(file, header[:])
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
			break // Clean EOF or partial write at end of segment
		}
		if err != nil {
			return nil, fmt.Errorf("failed reading header: %w", err)
		}

		expectedLen := binary.BigEndian.Uint32(header[0:4])
		expectedChecksum := binary.BigEndian.Uint32(header[4:8])

		payload := make([]byte, expectedLen)
		if _, err := io.ReadFull(file, payload); err != nil {
			// Incomplete write during system crash; stop replay at clean boundary
			break
		}

		// Verify CRC32 checksum
		if crc32.ChecksumIEEE(payload) != expectedChecksum {
			return nil, ErrCorruptRecord
		}

		var batch pulsewatchv1.MetricBatch
		if err := proto.Unmarshal(payload, &batch); err != nil {
			return nil, fmt.Errorf("failed unmarshaling batch: %w", err)
		}

		batches = append(batches, &batch)
	}

	return batches, nil
}
