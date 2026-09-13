// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spanmetricsconnector

import (
	"fmt"
	"sync"
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

// SPIKE: equivalence of the original buildMetrics path and the snapshot path.

// normalizedJSON sorts resources and data points (whose order comes from map iteration)
// by their attributes and returns the JSON encoding.
func normalizedJSON(t *testing.T, in pmetric.Metrics) string {
	m := pmetric.NewMetrics()
	in.CopyTo(m)
	key := func(attrs pcommon.Map) string { return fmt.Sprint(attrs.AsRaw()) }
	m.ResourceMetrics().Sort(func(a, b pmetric.ResourceMetrics) bool {
		return key(a.Resource().Attributes()) < key(b.Resource().Attributes())
	})
	for i := 0; i < m.ResourceMetrics().Len(); i++ {
		sms := m.ResourceMetrics().At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				switch mt := ms.At(k); mt.Type() {
				case pmetric.MetricTypeSum:
					mt.Sum().DataPoints().Sort(func(a, b pmetric.NumberDataPoint) bool { return key(a.Attributes()) < key(b.Attributes()) })
				case pmetric.MetricTypeHistogram:
					mt.Histogram().DataPoints().Sort(func(a, b pmetric.HistogramDataPoint) bool { return key(a.Attributes()) < key(b.Attributes()) })
				case pmetric.MetricTypeExponentialHistogram:
					mt.ExponentialHistogram().DataPoints().Sort(func(a, b pmetric.ExponentialHistogramDataPoint) bool {
						return key(a.Attributes()) < key(b.Attributes())
					})
				default:
					t.Fatalf("unexpected metric type %v", mt.Type())
				}
			}
		}
	}
	b, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(m)
	require.NoError(t, err)
	return string(b)
}

func newSnapTestConnector(t *testing.T, clock clockwork.Clock, temporality, histogram string, exemplars bool) *connectorImp {
	cfg := createDefaultConfig().(*Config)
	cfg.AggregationTemporality = temporality
	cfg.Dimensions = []Dimension{{Name: "http.request.method"}, {Name: "http.route"}}
	cfg.Events = EventsConfig{Enabled: true, Dimensions: []Dimension{{Name: "exception.type"}}}
	if histogram == "exponential" {
		cfg.Histogram.Exponential = configoptional.Some(ExponentialHistogramConfig{})
	}
	cfg.Exemplars.Enabled = exemplars
	cfg.Exemplars.MaxPerDataPoint = 5
	c, err := newConnector(zap.NewNop(), cfg, clock, instanceID)
	require.NoError(t, err)
	c.metricsConsumer = consumertest.NewNop()
	return c
}

// buildSnapTestTraces produces a growing set of series per round (so later rounds mix
// isFirst series with existing ones), some spans with events, and repeated spans per series.
func buildSnapTestTraces(round int) ptrace.Traces {
	td := ptrace.NewTraces()
	base := time.Unix(1_700_000_000, 0)
	for r := range 3 {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", r))
		rs.Resource().Attributes().PutStr("host.name", fmt.Sprintf("host-%d", r))
		spans := rs.ScopeSpans().AppendEmpty().Spans()
		for i := range 10 + round*5 {
			for rep := range 1 + i%3 {
				span := spans.AppendEmpty()
				route := fmt.Sprintf("/api/%d", i)
				span.SetName("GET " + route)
				span.SetKind(ptrace.SpanKindServer)
				if i%4 == 0 {
					span.Status().SetCode(ptrace.StatusCodeError)
				}
				span.SetTraceID(pcommon.TraceID{byte(round + 1), byte(r), byte(i), byte(rep)})
				span.SetSpanID(pcommon.SpanID{byte(round + 1), byte(i), byte(rep)})
				span.SetStartTimestamp(pcommon.NewTimestampFromTime(base))
				span.SetEndTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(1+i*rep*37) * time.Millisecond)))
				span.Attributes().PutStr("http.request.method", "GET")
				span.Attributes().PutStr("http.route", route)
				if i%5 == 0 {
					ev := span.Events().AppendEmpty()
					ev.SetName("exception")
					ev.Attributes().PutStr("exception.type", fmt.Sprintf("E%d", i%2))
				}
			}
		}
	}
	return td
}

func TestSnapshotFlushEquivalence(t *testing.T) {
	for _, temporality := range []string{benchCumulative, benchDelta} {
		for _, histogram := range []string{"explicit", "exponential"} {
			for _, exemplars := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/exemplars=%v", temporality, histogram, exemplars), func(t *testing.T) {
					start := time.Unix(1_700_000_000, 0)
					clockA, clockB := clockwork.NewFakeClockAt(start), clockwork.NewFakeClockAt(start)
					a := newSnapTestConnector(t, clockA, temporality, histogram, exemplars)
					b := newSnapTestConnector(t, clockB, temporality, histogram, exemplars)
					for round := range 4 {
						// Round 2 sends nothing, so cumulative re-emits unchanged state.
						if round != 2 {
							require.NoError(t, a.ConsumeTraces(t.Context(), buildSnapTestTraces(round)))
							require.NoError(t, b.ConsumeTraces(t.Context(), buildSnapTestTraces(round)))
						}
						clockA.Advance(time.Second)
						clockB.Advance(time.Second)

						want := flushCriticalSection(a)
						got := b.flushSnapshot()
						// Delta purges on every flush, so the empty round 2 legitimately yields nothing.
						if temporality != benchDelta || round != 2 {
							require.Positive(t, want.DataPointCount(), "round %d", round)
						}
						require.Equal(t, want.DataPointCount(), got.DataPointCount(), "round %d", round)
						require.JSONEq(t, normalizedJSON(t, want), normalizedJSON(t, got), "round %d", round)
					}
				})
			}
		}
	}
}

// TestSnapshotFlushConcurrentConsume is meant for -race: ConsumeTraces runs concurrently with
// the outside-lock pdata build of snapshot flushes.
func TestSnapshotFlushConcurrentConsume(t *testing.T) {
	for _, temporality := range []string{benchCumulative, benchDelta} {
		for _, histogram := range []string{"explicit", "exponential"} {
			t.Run(temporality+"/"+histogram, func(t *testing.T) {
				p := newSnapTestConnector(t, clockwork.NewFakeClock(), temporality, histogram, true)
				inputs := []ptrace.Traces{buildSnapTestTraces(0), buildSnapTestTraces(1), buildSnapTestTraces(3)}
				// Prime so the first flush has data even if the workers are not scheduled yet.
				require.NoError(t, p.ConsumeTraces(t.Context(), inputs[0]))
				stop := make(chan struct{})
				var wg sync.WaitGroup
				for w := range 2 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for i := 0; ; i++ {
							select {
							case <-stop:
								return
							default:
							}
							_ = p.ConsumeTraces(t.Context(), inputs[(i+w)%len(inputs)])
						}
					}()
				}
				total := 0
				for range 40 {
					m := p.flushSnapshot()
					total += m.DataPointCount()
					// Touch the output like an exporter would.
					_, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(m)
					require.NoError(t, err)
				}
				close(stop)
				wg.Wait()
				require.Positive(t, total)
			})
		}
	}
}
