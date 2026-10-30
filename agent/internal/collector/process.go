package collector

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	pulsewatchv1 "pulsewatch/proto/v1"

	"github.com/shirou/gopsutil/v4/process"
)

// ProcessCollector gathers top resource-consuming processes.
// CRITICAL: It strictly aggregates by process executable name and NEVER includes PID,
// eliminating time-series cardinality explosions.
type ProcessCollector struct {
	topN int
}

// NewProcessCollector constructs a new process collector limited to topN executables.
func NewProcessCollector(topN int) *ProcessCollector {
	if topN <= 0 {
		topN = 10
	}
	return &ProcessCollector{
		topN: topN,
	}
}

func (p *ProcessCollector) Name() string {
	return "process"
}

type procAggregate struct {
	name     string
	cpuPct   float64
	rssBytes uint64
}

func (p *ProcessCollector) Collect(ctx context.Context) ([]*pulsewatchv1.MetricPoint, error) {
	nowMs := time.Now().UnixMilli()

	procs, err := process.ProcessesWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed listing processes: %w", err)
	}

	// Concurrent worker pool to fetch process stats rapidly without blocking
	type rawProcStat struct {
		name     string
		cpuPct   float64
		rssBytes uint64
	}

	results := make(chan rawProcStat, len(procs))
	procChan := make(chan *process.Process, len(procs))

	for _, pr := range procs {
		if pr.Pid > 4 { // Skip system idle and kernel
			procChan <- pr
		}
	}
	close(procChan)

	numWorkers := 16
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pr := range procChan {
				select {
				case <-ctx.Done():
					return
				default:
				}

				name, err := pr.NameWithContext(ctx)
				if err != nil || name == "" {
					continue
				}
				name = strings.TrimSuffix(strings.ToLower(name), ".exe")

				// Get memory RSS first (fast syscall)
				memInfo, err := pr.MemoryInfoWithContext(ctx)
				if err != nil || memInfo == nil || memInfo.RSS == 0 {
					continue
				}

				cpuPct, _ := pr.CPUPercentWithContext(ctx)

				results <- rawProcStat{
					name:     name,
					cpuPct:   cpuPct,
					rssBytes: memInfo.RSS,
				}
			}
		}()
	}

	wg.Wait()
	close(results)

	// Aggregate metrics by executable name
	aggMap := make(map[string]*procAggregate)
	for stat := range results {
		if agg, exists := aggMap[stat.name]; exists {
			agg.cpuPct += stat.cpuPct
			agg.rssBytes += stat.rssBytes
		} else {
			aggMap[stat.name] = &procAggregate{
				name:     stat.name,
				cpuPct:   stat.cpuPct,
				rssBytes: stat.rssBytes,
			}
		}
	}

	// Sort by RSS memory to find top active applications
	aggList := make([]*procAggregate, 0, len(aggMap))
	for _, agg := range aggMap {
		aggList = append(aggList, agg)
	}

	sort.Slice(aggList, func(i, j int) bool {
		return aggList[i].rssBytes > aggList[j].rssBytes
	})

	limit := p.topN
	if len(aggList) < limit {
		limit = len(aggList)
	}

	var points []*pulsewatchv1.MetricPoint
	for i := 0; i < limit; i++ {
		agg := aggList[i]
		labels := map[string]string{
			"process_name": agg.name,
		}

		points = append(points,
			&pulsewatchv1.MetricPoint{
				Name:            "process.cpu.utilization",
				Value:           agg.cpuPct,
				TimestampUnixMs: nowMs,
				Labels:          labels,
			},
			&pulsewatchv1.MetricPoint{
				Name:            "process.memory.rss_bytes",
				Value:           float64(agg.rssBytes),
				TimestampUnixMs: nowMs,
				Labels:          labels,
			},
		)
	}

	return points, nil
}
