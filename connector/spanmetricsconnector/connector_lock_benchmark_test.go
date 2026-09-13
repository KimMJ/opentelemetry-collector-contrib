// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spanmetricsconnector

import (
	"fmt"
	"os"
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

// SPIKE: SPANMETRICS_BENCH_SNAPSHOT=1 switches the original-named benchmarks to the snapshot
// flush path, so that two runs with identical sub-benchmark names can be compared by benchstat.
var benchSnapshotMode = os.Getenv("SPANMETRICS_BENCH_SNAPSHOT") != ""

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

// flushFull runs a complete flush with the selected path.
func flushFull(p *connectorImp, snapshot bool) pmetric.Metrics {
	if snapshot {
		return p.flushSnapshot()
	}
	return flushCriticalSection(p)
}

// BenchmarkFlushCriticalSection measures how long a single flush holds p.lock.
// With SPANMETRICS_BENCH_SNAPSHOT=1 only snapshot+resetState is timed; the outside-lock
// build is excluded from the timer and reported as build-ms/op.
func BenchmarkFlushCriticalSection(b *testing.B) { benchFlushCriticalSection(b, benchSnapshotMode) }

func BenchmarkFlushCriticalSectionSnapshot(b *testing.B) { benchFlushCriticalSection(b, true) }

func benchFlushCriticalSection(b *testing.B, snapshot bool) {
	for _, tc := range flushBenchCases() {
		b.Run(tc.name(), func(b *testing.B) {
			p := newFlushBenchConnector(b, tc.temporality, tc.histogram, tc.resources)
			td := buildFlushBenchTraces(tc.resources, tc.series)
			ctx := b.Context()
			require.NoError(b, p.ConsumeTraces(ctx, td))
			// Warm-up flush: moves cumulative sums past isFirst and gives the data point count.
			dataPoints := flushFull(p, snapshot).DataPointCount()
			isDelta := tc.temporality == benchDelta

			var build time.Duration
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if isDelta {
					// Delta purges state on every flush, so refill it outside the timer.
					b.StopTimer()
					require.NoError(b, p.ConsumeTraces(ctx, td))
					b.StartTimer()
				}
				if !snapshot {
					flushBenchSink = flushCriticalSection(p)
					continue
				}
				p.lock.Lock()
				ts, snaps := p.snapshotLocked()
				p.resetState()
				p.lock.Unlock()
				b.StopTimer()
				start := time.Now()
				flushBenchSink = p.buildMetricsFromSnapshot(ts, snaps)
				build += time.Since(start)
				b.StartTimer()
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(dataPoints), "ns/datapoint")
			b.ReportMetric(float64(dataPoints), "datapoints")
			if snapshot {
				b.ReportMetric(float64(build.Microseconds())/float64(b.N)/1e3, "build-ms/op")
			}
		})
	}
}

// BenchmarkFlushSnapshotBuildOutsideLock times only the outside-lock pdata build.
func BenchmarkFlushSnapshotBuildOutsideLock(b *testing.B) {
	for _, tc := range flushBenchCases() {
		b.Run(tc.name(), func(b *testing.B) {
			p := newFlushBenchConnector(b, tc.temporality, tc.histogram, tc.resources)
			td := buildFlushBenchTraces(tc.resources, tc.series)
			ctx := b.Context()
			require.NoError(b, p.ConsumeTraces(ctx, td))
			p.flushSnapshot()
			isDelta := tc.temporality == benchDelta
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				if isDelta {
					require.NoError(b, p.ConsumeTraces(ctx, td))
				}
				p.lock.Lock()
				ts, snaps := p.snapshotLocked()
				p.resetState()
				p.lock.Unlock()
				b.StartTimer()
				flushBenchSink = p.buildMetricsFromSnapshot(ts, snaps)
			}
		})
	}
}

// BenchmarkFlushTotal times a complete flush (locked + unlocked parts) to compare total work.
func BenchmarkFlushTotal(b *testing.B) {
	for _, tc := range flushBenchCases() {
		b.Run(tc.name(), func(b *testing.B) {
			p := newFlushBenchConnector(b, tc.temporality, tc.histogram, tc.resources)
			td := buildFlushBenchTraces(tc.resources, tc.series)
			ctx := b.Context()
			require.NoError(b, p.ConsumeTraces(ctx, td))
			flushFull(p, benchSnapshotMode)
			isDelta := tc.temporality == benchDelta
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if isDelta {
					b.StopTimer()
					require.NoError(b, p.ConsumeTraces(ctx, td))
					b.StartTimer()
				}
				flushBenchSink = flushFull(p, benchSnapshotMode)
			}
		})
	}
}

// BenchmarkConsumeTracesWaitDuringFlush measures how long a ConsumeTraces call that
// arrives right after a flush takes the lock is blocked.
func BenchmarkConsumeTracesWaitDuringFlush(b *testing.B) {
	benchConsumeTracesWait(b, benchSnapshotMode)
}

func BenchmarkConsumeTracesWaitDuringFlushSnapshot(b *testing.B) { benchConsumeTracesWait(b, true) }

func benchConsumeTracesWait(b *testing.B, snapshot bool) {
	for _, tc := range flushBenchCases() {
		if tc.temporality != benchCumulative {
			continue
		}
		b.Run(tc.name(), func(b *testing.B) {
			p := newFlushBenchConnector(b, tc.temporality, tc.histogram, tc.resources)
			ctx := b.Context()
			require.NoError(b, p.ConsumeTraces(ctx, buildFlushBenchTraces(tc.resources, tc.series)))
			flushFull(p, snapshot)
			// A single span hitting an existing series, like a steady-state export request.
			small := buildFlushBenchTraces(1, 1)

			var total, maxWait time.Duration
			b.ResetTimer()
			for range b.N {
				held := make(chan struct{})
				done := make(chan struct{})
				go func() {
					if snapshot {
						p.flushLock.Lock()
						p.lock.Lock()
						close(held)
						ts, snaps := p.snapshotLocked()
						p.resetState()
						p.lock.Unlock()
						flushBenchSink = p.buildMetricsFromSnapshot(ts, snaps)
						p.flushLock.Unlock()
					} else {
						p.lock.Lock()
						close(held)
						m := p.buildMetrics()
						p.resetState()
						p.lock.Unlock()
						flushBenchSink = m
					}
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
