// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spanmetricsconnector

import (
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

const (
	benchCumulative = "AGGREGATION_TEMPORALITY_CUMULATIVE"
	benchDelta      = "AGGREGATION_TEMPORALITY_DELTA"
)

var flushBenchSink pmetric.Metrics

type flushBenchCase struct {
	temporality string
	histogram   string
	resources   int
	series      int
}

func (c flushBenchCase) name() string {
	t := "cumulative"
	if c.temporality == benchDelta {
		t = "delta"
	}
	return fmt.Sprintf("temporality=%s/histogram=%s/resources=%d/series=%d", t, c.histogram, c.resources, c.series)
}

func flushBenchSizes() [][2]int {
	sizes := [][2]int{{100, 1_000}, {100, 10_000}, {100, 100_000}, {1_000, 100_000}}
	if os.Getenv("SPANMETRICS_BENCH_LARGE") != "" {
		sizes = append(sizes, [2]int{1_000, 500_000})
	}
	return sizes
}

func flushBenchCases() []flushBenchCase {
	var cases []flushBenchCase
	for _, temporality := range []string{benchCumulative, benchDelta} {
		for _, histogram := range []string{"explicit", "exponential"} {
			for _, size := range flushBenchSizes() {
				cases = append(cases, flushBenchCase{temporality, histogram, size[0], size[1]})
			}
		}
	}
	return cases
}

// newFlushBenchConnector builds a connector from the default config, with two extra
// dimensions to resemble a typical HTTP service setup.
func newFlushBenchConnector(tb testing.TB, temporality, histogram string, resources int) *connectorImp {
	cfg := createDefaultConfig().(*Config)
	cfg.AggregationTemporality = temporality
	cfg.ResourceMetricsCacheSize = resources
	cfg.Dimensions = []Dimension{{Name: "http.request.method"}, {Name: "http.route"}}
	if histogram == "exponential" {
		cfg.Histogram.Exponential = configoptional.Some(ExponentialHistogramConfig{})
	}
	c, err := newConnector(zap.NewNop(), cfg, clockwork.NewFakeClock(), instanceID)
	require.NoError(tb, err)
	c.metricsConsumer = consumertest.NewNop()
	return c
}

// buildFlushBenchTraces returns traces producing `series` distinct series per metric,
// spread evenly across `resources` distinct resources.
func buildFlushBenchTraces(resources, series int) ptrace.Traces {
	td := ptrace.NewTraces()
	perResource := series / resources
	now := time.Now()
	for r := range resources {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", r))
		rs.Resource().Attributes().PutStr("host.name", fmt.Sprintf("host-%d", r))
		spans := rs.ScopeSpans().AppendEmpty().Spans()
		spans.EnsureCapacity(perResource)
		for i := range perResource {
			span := spans.AppendEmpty()
			route := fmt.Sprintf("/api/v1/items/%d", i)
			span.SetName("GET " + route)
			span.SetKind(ptrace.SpanKindServer)
			span.SetTraceID(pcommon.TraceID{1, byte(r), byte(r >> 8), byte(i), byte(i >> 8), byte(i >> 16)})
			span.SetSpanID(pcommon.SpanID{1, byte(i), byte(i >> 8), byte(i >> 16)})
			span.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-time.Duration(1+i%2000) * time.Millisecond)))
			span.SetEndTimestamp(pcommon.NewTimestampFromTime(now))
			span.Attributes().PutStr("http.request.method", "GET")
			span.Attributes().PutStr("http.route", route)
		}
	}
	return td
}

// flushCriticalSection mirrors the locked part of exportMetrics.
func flushCriticalSection(p *connectorImp) pmetric.Metrics {
	p.lock.Lock()
	m := p.buildMetrics()
	p.resetState()
	p.lock.Unlock()
	return m
}

// BenchmarkFlushCriticalSection measures how long a single flush holds p.lock.
func BenchmarkFlushCriticalSection(b *testing.B) {
	for _, tc := range flushBenchCases() {
		b.Run(tc.name(), func(b *testing.B) {
			p := newFlushBenchConnector(b, tc.temporality, tc.histogram, tc.resources)
			td := buildFlushBenchTraces(tc.resources, tc.series)
			ctx := b.Context()
			require.NoError(b, p.ConsumeTraces(ctx, td))
			// Warm-up flush: moves cumulative sums past isFirst and gives the data point count.
			dataPoints := flushCriticalSection(p).DataPointCount()
			isDelta := tc.temporality == benchDelta

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if isDelta {
					// Delta purges state on every flush, so refill it outside the timer.
					b.StopTimer()
					require.NoError(b, p.ConsumeTraces(ctx, td))
					b.StartTimer()
				}
				flushBenchSink = flushCriticalSection(p)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(dataPoints), "ns/datapoint")
			b.ReportMetric(float64(dataPoints), "datapoints")
		})
	}
}

// BenchmarkConsumeTracesWaitDuringFlush measures how long a ConsumeTraces call that
// arrives right after a flush takes the lock is blocked.
func BenchmarkConsumeTracesWaitDuringFlush(b *testing.B) {
	for _, tc := range flushBenchCases() {
		if tc.temporality != benchCumulative {
			continue
		}
		b.Run(tc.name(), func(b *testing.B) {
			p := newFlushBenchConnector(b, tc.temporality, tc.histogram, tc.resources)
			ctx := b.Context()
			require.NoError(b, p.ConsumeTraces(ctx, buildFlushBenchTraces(tc.resources, tc.series)))
			flushCriticalSection(p)
			// A single span hitting an existing series, like a steady-state export request.
			small := buildFlushBenchTraces(1, 1)

			var total, maxWait time.Duration
			b.ResetTimer()
			for range b.N {
				held := make(chan struct{})
				done := make(chan struct{})
				go func() {
					p.lock.Lock()
					close(held)
					m := p.buildMetrics()
					p.resetState()
					p.lock.Unlock()
					flushBenchSink = m
					close(done)
				}()
				<-held
				start := time.Now()
				require.NoError(b, p.ConsumeTraces(ctx, small))
				wait := time.Since(start)
				<-done
				total += wait
				maxWait = max(maxWait, wait)
			}
			b.ReportMetric(float64(total.Microseconds())/float64(b.N)/1e3, "wait-ms/op")
			b.ReportMetric(float64(maxWait.Microseconds())/1e3, "max-wait-ms")
		})
	}
}

// buildIngestBatch returns one export request: batchSize spans from a single resource,
// cycling over routes that already exist as series (steady state, no new series).
func buildIngestBatch(resource, routes, batchSize int) ptrace.Traces {
	td := ptrace.NewTraces()
	now := time.Now()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", resource))
	rs.Resource().Attributes().PutStr("host.name", fmt.Sprintf("host-%d", resource))
	spans := rs.ScopeSpans().AppendEmpty().Spans()
	spans.EnsureCapacity(batchSize)
	for i := range batchSize {
		route := fmt.Sprintf("/api/v1/items/%d", i%routes)
		span := spans.AppendEmpty()
		span.SetName("GET " + route)
		span.SetKind(ptrace.SpanKindServer)
		span.SetTraceID(pcommon.TraceID{2, byte(resource), byte(i), byte(i >> 8)})
		span.SetSpanID(pcommon.SpanID{2, byte(i), byte(i >> 8)})
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-time.Duration(1+i%2000) * time.Millisecond)))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(now))
		span.Attributes().PutStr("http.request.method", "GET")
		span.Attributes().PutStr("http.route", route)
	}
	return td
}

// BenchmarkConsumeTracesParallel measures ingest throughput when several goroutines call
// ConsumeTraces at once, with no flush running. Run with -cpu 1,2,4,8.
//   - shared:  one connector for all goroutines (current design, one p.lock).
//   - sharded: one connector per goroutine, an upper bound for what splitting the lock could reach.
func BenchmarkConsumeTracesParallel(b *testing.B) {
	const (
		resources = 100
		routes    = 100
		batchSize = 512
	)
	newConn := func() *connectorImp {
		p := newFlushBenchConnector(b, benchCumulative, "explicit", resources)
		require.NoError(b, p.ConsumeTraces(b.Context(), buildFlushBenchTraces(resources, resources*routes)))
		return p
	}
	batches := make([]ptrace.Traces, resources)
	for r := range resources {
		batches[r] = buildIngestBatch(r, routes, batchSize)
	}

	for _, mode := range []string{"shared", "sharded"} {
		b.Run("mode="+mode, func(b *testing.B) {
			conns := make([]*connectorImp, runtime.GOMAXPROCS(0))
			if mode == "shared" {
				shared := newConn()
				for i := range conns {
					conns[i] = shared
				}
			} else {
				for i := range conns {
					conns[i] = newConn()
				}
			}
			var workerID atomic.Int64
			ctx := b.Context()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				id := int(workerID.Add(1) - 1)
				p := conns[id%len(conns)]
				for n := id; pb.Next(); n++ {
					if err := p.ConsumeTraces(ctx, batches[n%resources]); err != nil {
						b.Error(err)
						return
					}
				}
			})
			b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "spans/s")
		})
	}
}
