// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spanmetricsconnector

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func init() {
	spikePreAgg = os.Getenv("SPANMETRICS_BENCH_PREAGG") != ""
}

// flattenMetrics turns metrics into sorted "resource|metric|attrs => values" lines.
func flattenMetrics(m pmetric.Metrics) []string {
	var lines []string
	rms := m.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		res := fmt.Sprint(rm.Resource().Attributes().AsRaw())
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				metric := ms.At(k)
				prefix := res + "|" + metric.Name() + "|"
				switch metric.Type() {
				case pmetric.MetricTypeSum:
					dps := metric.Sum().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						lines = append(lines, fmt.Sprintf("%s%v => start=%d v=%d", prefix, dp.Attributes().AsRaw(), dp.StartTimestamp(), dp.IntValue()))
					}
				case pmetric.MetricTypeHistogram:
					dps := metric.Histogram().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						lines = append(lines, fmt.Sprintf("%s%v => start=%d c=%d s=%v b=%v", prefix, dp.Attributes().AsRaw(), dp.StartTimestamp(), dp.Count(), dp.Sum(), dp.BucketCounts().AsRaw()))
					}
				case pmetric.MetricTypeExponentialHistogram:
					dps := metric.ExponentialHistogram().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						lines = append(lines, fmt.Sprintf("%s%v => start=%d c=%d s=%v z=%d sc=%d po=%d p=%v", prefix, dp.Attributes().AsRaw(), dp.StartTimestamp(), dp.Count(), dp.Sum(), dp.ZeroCount(), dp.Scale(), dp.Positive().Offset(), dp.Positive().BucketCounts().AsRaw()))
					}
				}
			}
		}
	}
	sort.Strings(lines)
	return lines
}

func flushForTest(p *connectorImp) pmetric.Metrics {
	p.lock.Lock()
	defer p.lock.Unlock()
	m := p.buildMetrics()
	p.resetState()
	return m
}

func TestPreAggEquivalence(t *testing.T) {
	type variant struct {
		name string
		make func(clock clockwork.Clock) *connectorImp
		data func(round int) ptrace.Traces
	}
	benchData := func(round int) ptrace.Traces {
		td := buildFlushBenchTraces(10, 200+round*50)
		// Same resource repeated in one request, plus an extra batch.
		buildIngestBatch(3, 30, 64).ResourceSpans().MoveAndAppendTo(td.ResourceSpans())
		buildIngestBatch(3, 30, 64).ResourceSpans().MoveAndAppendTo(td.ResourceSpans())
		return td
	}
	var variants []variant
	for _, temporality := range []string{benchCumulative, benchDelta} {
		for _, histogram := range []string{"explicit", "exponential"} {
			variants = append(variants, variant{
				name: fmt.Sprintf("bench/%s/%s", temporality, histogram),
				make: func(clock clockwork.Clock) *connectorImp {
					p := newFlushBenchConnector(t, temporality, histogram, 100)
					p.clock = clock
					return p
				},
				data: benchData,
			})
		}
	}
	for _, temporality := range []string{cumulative, delta} {
		variants = append(variants, variant{
			name: "sample/" + temporality,
			make: func(clock clockwork.Clock) *connectorImp {
				p, err := newConnectorImp(new("defaultNullValue"), explicitHistogramsConfig, disabledExemplarsConfig, disabledEventsConfig, temporality, 0, []string{}, 1000, clock, false)
				require.NoError(t, err)
				return p
			},
			data: func(int) ptrace.Traces { return buildSampleTrace() },
		})
	}

	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			clock := clockwork.NewFakeClock()
			orig, pre := v.make(clock), v.make(clock)
			for round := range 3 {
				td := v.data(round)
				orig.lock.Lock()
				orig.aggregateMetrics(td)
				orig.lock.Unlock()
				require.NoError(t, pre.consumeTracesPreAgg(td))
				want, got := flattenMetrics(flushForTest(orig)), flattenMetrics(flushForTest(pre))
				require.NotEmpty(t, want)
				require.Equal(t, want, got, "round %d", round)
			}
		})
	}
}

func TestPreAggConcurrent(t *testing.T) {
	p := newFlushBenchConnector(t, benchCumulative, "explicit", 100)
	batches := make([]ptrace.Traces, 8)
	for i := range batches {
		batches[i] = buildIngestBatch(i, 50, 128)
	}
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for i := range 50 {
				require.NoError(t, p.consumeTracesPreAgg(batches[(w+i)%len(batches)]))
			}
		})
	}
	wg.Go(func() {
		for range 20 {
			flushForTest(p)
		}
	})
	wg.Wait()
}
