// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spanmetricsconnector

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const (
	holdResources = 100
	holdRoutes    = 512 // series pre-populated per resource
	holdBatchSize = 512 // spans per request
)

// newHoldBenchConnector returns a cumulative/explicit connector pre-populated with
// holdResources × holdRoutes series, so fixed batches only hit existing series.
func newHoldBenchConnector(b *testing.B) *connectorImp {
	p := newFlushBenchConnector(b, benchCumulative, "explicit", holdResources)
	p.lock.Lock()
	p.aggregateMetrics(buildFlushBenchTraces(holdResources, holdResources*holdRoutes))
	p.lock.Unlock()
	return p
}

// buildNewSeriesBatch returns a request whose spans all map to series never seen before.
func buildNewSeriesBatch(resource, seq, batchSize int) ptrace.Traces {
	td := buildIngestBatch(resource, batchSize, batchSize)
	spans := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	for i := 0; i < spans.Len(); i++ {
		route := fmt.Sprintf("/new/%d/%d", seq, i)
		spans.At(i).SetName("GET " + route)
		spans.At(i).Attributes().PutStr("http.route", route)
	}
	return td
}

// BenchmarkLockHoldPerRequest measures, with a single goroutine (no contention), how long one
// request holds p.lock. distinct = number of distinct series the 512 spans of a request map to;
// "new" = every span creates a new series. Use -benchtime Nx for "new" (state grows with b.N).
func BenchmarkLockHoldPerRequest(b *testing.B) {
	for _, distinct := range []string{"512", "100", "10", "new"} {
		for _, impl := range []string{"current", "preagg"} {
			b.Run(fmt.Sprintf("distinct=%s/impl=%s", distinct, impl), func(b *testing.B) {
				p := newHoldBenchConnector(b)
				var fixed []ptrace.Traces
				if distinct != "new" {
					d, err := strconv.Atoi(distinct)
					require.NoError(b, err)
					fixed = make([]ptrace.Traces, holdResources)
					for r := range holdResources {
						fixed[r] = buildIngestBatch(r, d, holdBatchSize)
					}
				}

				var hold, outside time.Duration
				b.ResetTimer()
				for i := range b.N {
					var td ptrace.Traces
					if fixed == nil {
						b.StopTimer()
						td = buildNewSeriesBatch(i%holdResources, i, holdBatchSize)
						b.StartTimer()
					} else {
						td = fixed[i%holdResources]
					}
					if impl == "current" {
						start := time.Now()
						p.lock.Lock()
						p.aggregateMetrics(td)
						p.lock.Unlock()
						hold += time.Since(start)
					} else {
						start := time.Now()
						batch := p.preAggregate(td)
						mid := time.Now()
						p.lock.Lock()
						p.applyPreAggregated(batch)
						p.lock.Unlock()
						hold += time.Since(mid)
						outside += mid.Sub(start)
					}
				}
				holdNs := float64(hold.Nanoseconds()) / float64(b.N)
				b.ReportMetric(holdNs/1e3, "lock-us/req")
				b.ReportMetric(float64(outside.Nanoseconds())/float64(b.N)/1e3, "outside-us/req")
				b.ReportMetric(float64(holdBatchSize)/(holdNs/1e9), "ceiling-spans/s")
			})
		}
	}
}

// BenchmarkConsumeTracesParallelDistinct is BenchmarkConsumeTracesParallel (shared mode) with
// a varying number of distinct series per request. Toggle option A with SPANMETRICS_BENCH_PREAGG=1.
func BenchmarkConsumeTracesParallelDistinct(b *testing.B) {
	for _, d := range []int{512, 100, 10} {
		b.Run(fmt.Sprintf("distinct=%d", d), func(b *testing.B) {
			p := newHoldBenchConnector(b)
			batches := make([]ptrace.Traces, holdResources)
			for r := range holdResources {
				batches[r] = buildIngestBatch(r, d, holdBatchSize)
			}
			var workerID atomic.Int64
			ctx := b.Context()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				id := int(workerID.Add(1) - 1)
				for n := id; pb.Next(); n++ {
					if err := p.ConsumeTraces(ctx, batches[n%holdResources]); err != nil {
						b.Error(err)
						return
					}
				}
			})
			b.ReportMetric(float64(b.N*holdBatchSize)/b.Elapsed().Seconds(), "spans/s")
		})
	}
}
