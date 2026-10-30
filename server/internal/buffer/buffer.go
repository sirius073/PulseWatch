package buffer

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	pulsewatchv1 "pulsewatch/proto/v1"
	"pulsewatch/server/internal/storage"
)

var (
	// ErrBufferFull is returned when the incoming channel is saturated, providing backpressure.
	ErrBufferFull = errors.New("buffer: ingest queue capacity exceeded (backpressure)")
)

// IngestTask encapsulates a metric batch and a one-shot response channel for deferred acks.
type IngestTask struct {
	Batch    *pulsewatchv1.MetricBatch
	Response chan IngestResult
}

// IngestResult carries the commit status back to the gRPC handler.
type IngestResult struct {
	Status pulsewatchv1.AckStatus
	Err    error
}

// Queue manages a bounded channel and a pool of database write workers.
type Queue struct {
	tasks      chan IngestTask
	store      *storage.Storage
	workerPool int
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewQueue constructs a bounded worker queue.
// capacity: size of the buffer channel (e.g. 5,000 tasks).
// workers: number of concurrent DB commit workers (e.g. 8).
func NewQueue(store *storage.Storage, capacity int, workers int) *Queue {
	if capacity <= 0 {
		capacity = 5000
	}
	if workers <= 0 {
		workers = 8
	}

	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		tasks:      make(chan IngestTask, capacity),
		store:      store,
		workerPool: workers,
		ctx:        ctx,
		cancel:     cancel,
	}

	q.startWorkers()
	return q
}

// startWorkers launches the worker goroutines.
func (q *Queue) startWorkers() {
	for i := 0; i < q.workerPool; i++ {
		q.wg.Add(1)
		go q.workerLoop(i)
	}
}

// workerLoop drains batches from the channel and commits them to TimescaleDB.
func (q *Queue) workerLoop(workerID int) {
	defer q.wg.Done()

	for {
		select {
		case <-q.ctx.Done():
			return

		case task, ok := <-q.tasks:
			if !ok {
				return
			}

			// Commit to TimescaleDB with idempotency
			commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			status, err := q.store.IngestBatch(commitCtx, task.Batch)
			cancel()

			// Signal gRPC caller that database commit has completed (deferred ack)
			task.Response <- IngestResult{
				Status: status,
				Err:    err,
			}
		}
	}
}

// EnqueueAndWait accepts a batch into the queue and blocks until the worker commits to PostgreSQL.
// If the buffer is full, it immediately returns ErrBufferFull without allocating, shedding load cleanly.
func (q *Queue) EnqueueAndWait(ctx context.Context, batch *pulsewatchv1.MetricBatch) (pulsewatchv1.AckStatus, error) {
	respChan := make(chan IngestResult, 1)
	task := IngestTask{
		Batch:    batch,
		Response: respChan,
	}

	// Non-blocking enqueue for backpressure load-shedding
	select {
	case q.tasks <- task:
		// Successfully queued, now await DB commit confirmation
	default:
		// Buffer is full: shed load with backpressure error
		return pulsewatchv1.AckStatus_ACK_STATUS_RETRYABLE_ERROR, ErrBufferFull
	}

	select {
	case <-ctx.Done():
		return pulsewatchv1.AckStatus_ACK_STATUS_RETRYABLE_ERROR, ctx.Err()
	case res := <-respChan:
		if res.Err != nil {
			return pulsewatchv1.AckStatus_ACK_STATUS_RETRYABLE_ERROR, res.Err
		}
		return res.Status, nil
	}
}

// Depth returns the number of currently enqueued tasks.
func (q *Queue) Depth() int {
	return len(q.tasks)
}

// Close gracefully stops the worker pool.
func (q *Queue) Close() {
	q.cancel()
	close(q.tasks)
	q.wg.Wait()
	log.Println("[Queue] Workers stopped gracefully")
}
