// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metrics // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/spanmetricsconnector/internal/metrics"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// SPIKE: lightweight value snapshots taken under the connector lock, so that pdata
// construction can happen outside of it.
//
// Ownership rules relied upon:
//   - attributes maps are built once in GetOrCreate and never mutated afterwards, so the
//     snapshot shares them (no deep copy) and they are only read outside the lock.
//   - exemplar slices are *taken over* by the snapshot when exemplars are enabled: right
//     after the snapshot, resetState either replaces every series' slice with a fresh one
//     (cumulative, ClearExemplars) or drops all series (delta, Purge), so ConsumeTraces never
//     touches a slice owned by a snapshot.

// StartTimestampGenerator matches the generator used by BuildMetrics.
type StartTimestampGenerator func(Key, pcommon.Timestamp) pcommon.Timestamp

// HistogramSnapshot is an immutable copy of histogram values that can be turned into pdata
// without holding the connector lock.
type HistogramSnapshot interface {
	BuildMetrics(pmetric.Metric, pcommon.Timestamp, StartTimestampGenerator, pmetric.AggregationTemporality)
}

// SumPoint is a snapshot of a single Sum series.
type SumPoint struct {
	key            Key
	attributes     pcommon.Map
	startTimestamp pcommon.Timestamp
	value          int64
	exemplars      pmetric.ExemplarSlice
	hasExemplars   bool
}

// Snapshot appends the current series values to dst. For cumulative temporality it applies
// the isFirst "emit 0 first" rule and clears isFirst, exactly like BuildMetrics does.
// Must be called under the connector lock.
func (m *SumMetrics) Snapshot(dst []SumPoint, temporality pmetric.AggregationTemporality, withExemplars bool) []SumPoint {
	if cap(dst)-len(dst) < len(m.metrics) {
		n := make([]SumPoint, len(dst), len(dst)+len(m.metrics))
		copy(n, dst)
		dst = n
	}
	cumulative := temporality == pmetric.AggregationTemporalityCumulative
	for k, s := range m.metrics {
		p := SumPoint{key: k, attributes: s.attributes, startTimestamp: s.startTimestamp, value: int64(s.count)}
		if cumulative && s.isFirst {
			p.value = 0
			s.isFirst = false
		}
		if withExemplars && s.exemplars.Len() > 0 {
			p.exemplars = s.exemplars
			p.hasExemplars = true
		}
		dst = append(dst, p)
	}
	return dst
}

// BuildSumMetricsFromSnapshot builds the pdata metric from a snapshot. Safe outside the lock.
func BuildSumMetricsFromSnapshot(
	metric pmetric.Metric,
	points []SumPoint,
	timestamp pcommon.Timestamp,
	startTimeStampGenerator StartTimestampGenerator,
	temporality pmetric.AggregationTemporality,
) {
	metric.SetEmptySum().SetIsMonotonic(true)
	metric.Sum().SetAggregationTemporality(temporality)
	dps := metric.Sum().DataPoints()
	dps.EnsureCapacity(len(points))
	for i := range points {
		p := &points[i]
		dp := dps.AppendEmpty()
		dp.SetStartTimestamp(startTimeStampGenerator(p.key, p.startTimestamp))
		dp.SetTimestamp(timestamp)
		dp.SetIntValue(p.value)
		moveExemplars(p.hasExemplars, p.exemplars, timestamp, dp.Exemplars())
		p.attributes.CopyTo(dp.Attributes())
	}
}

func moveExemplars(has bool, src pmetric.ExemplarSlice, timestamp pcommon.Timestamp, dest pmetric.ExemplarSlice) {
	if !has {
		return
	}
	for i := 0; i < src.Len(); i++ {
		src.At(i).SetTimestamp(timestamp)
	}
	src.MoveAndAppendTo(dest)
}

type explicitPoint struct {
	key            Key
	attributes     pcommon.Map
	startTimestamp pcommon.Timestamp
	count          uint64
	sum            float64
	bucketCounts   []uint64 // sub-slice of a flat per-snapshot buffer
	exemplars      pmetric.ExemplarSlice
	hasExemplars   bool
}

type explicitHistogramSnapshot struct {
	bounds []float64 // shared, read-only
	points []explicitPoint
}

func (m *explicitHistogramMetrics) Snapshot(withExemplars bool) HistogramSnapshot {
	nb := len(m.bounds) + 1
	flat := make([]uint64, len(m.metrics)*nb)
	s := &explicitHistogramSnapshot{bounds: m.bounds, points: make([]explicitPoint, 0, len(m.metrics))}
	i := 0
	for k, h := range m.metrics {
		b := flat[i*nb : (i+1)*nb : (i+1)*nb]
		copy(b, h.bucketCounts)
		i++
		p := explicitPoint{key: k, attributes: h.attributes, startTimestamp: h.startTimestamp, count: h.count, sum: h.sum, bucketCounts: b}
		if withExemplars && h.exemplars.Len() > 0 {
			p.exemplars = h.exemplars
			p.hasExemplars = true
		}
		s.points = append(s.points, p)
	}
	return s
}

func (s *explicitHistogramSnapshot) BuildMetrics(
	metric pmetric.Metric,
	timestamp pcommon.Timestamp,
	startTimeStampGenerator StartTimestampGenerator,
	temporality pmetric.AggregationTemporality,
) {
	metric.SetEmptyHistogram().SetAggregationTemporality(temporality)
	dps := metric.Histogram().DataPoints()
	dps.EnsureCapacity(len(s.points))
	for i := range s.points {
		p := &s.points[i]
		dp := dps.AppendEmpty()
		dp.SetStartTimestamp(startTimeStampGenerator(p.key, p.startTimestamp))
		dp.SetTimestamp(timestamp)
		dp.ExplicitBounds().FromRaw(s.bounds)
		dp.BucketCounts().FromRaw(p.bucketCounts)
		dp.SetCount(p.count)
		dp.SetSum(p.sum)
		moveExemplars(p.hasExemplars, p.exemplars, timestamp, dp.Exemplars())
		p.attributes.CopyTo(dp.Attributes())
	}
}

// expoPoint holds the exact values expoHistToExponentialDataPoint reads, instead of a
// structure.Histogram copy (CopyInto = Clear+MergeFrom, which re-inserts bucket by bucket).
type expoPoint struct {
	key            Key
	attributes     pcommon.Map
	startTimestamp pcommon.Timestamp
	count          uint64
	sum, min, max  float64
	zeroCount      uint64
	scale          int32
	posOffset      int32
	negOffset      int32
	posStart       int
	posEnd         int
	negEnd         int // neg buckets are flat[posEnd:negEnd]
	exemplars      pmetric.ExemplarSlice
	hasExemplars   bool
}

type exponentialHistogramSnapshot struct {
	flat   []uint64
	points []expoPoint
}

func (m *exponentialHistogramMetrics) Snapshot(withExemplars bool) HistogramSnapshot {
	s := &exponentialHistogramSnapshot{points: make([]expoPoint, 0, len(m.metrics))}
	// Pre-size the flat buffer.
	total := 0
	for _, e := range m.metrics {
		total += int(e.histogram.Positive().Len() + e.histogram.Negative().Len())
	}
	s.flat = make([]uint64, 0, total)
	for k, e := range m.metrics {
		agg := e.histogram
		p := expoPoint{
			key: k, attributes: e.attributes, startTimestamp: e.startTimestamp,
			count: agg.Count(), sum: agg.Sum(), zeroCount: agg.ZeroCount(), scale: agg.Scale(),
		}
		if p.count != 0 {
			p.min = agg.Min()
			p.max = agg.Max()
		}
		pos, neg := agg.Positive(), agg.Negative()
		p.posOffset, p.negOffset = pos.Offset(), neg.Offset()
		p.posStart = len(s.flat)
		for i := uint32(0); i < pos.Len(); i++ {
			s.flat = append(s.flat, pos.At(i))
		}
		p.posEnd = len(s.flat)
		for i := uint32(0); i < neg.Len(); i++ {
			s.flat = append(s.flat, neg.At(i))
		}
		p.negEnd = len(s.flat)
		if withExemplars && e.exemplars.Len() > 0 {
			p.exemplars = e.exemplars
			p.hasExemplars = true
		}
		s.points = append(s.points, p)
	}
	return s
}

func (s *exponentialHistogramSnapshot) BuildMetrics(
	metric pmetric.Metric,
	timestamp pcommon.Timestamp,
	startTimeStampGenerator StartTimestampGenerator,
	temporality pmetric.AggregationTemporality,
) {
	metric.SetEmptyExponentialHistogram().SetAggregationTemporality(temporality)
	dps := metric.ExponentialHistogram().DataPoints()
	dps.EnsureCapacity(len(s.points))
	for i := range s.points {
		p := &s.points[i]
		dp := dps.AppendEmpty()
		dp.SetStartTimestamp(startTimeStampGenerator(p.key, p.startTimestamp))
		dp.SetTimestamp(timestamp)
		dp.SetCount(p.count)
		dp.SetSum(p.sum)
		if p.count != 0 {
			dp.SetMin(p.min)
			dp.SetMax(p.max)
		}
		dp.SetZeroCount(p.zeroCount)
		dp.SetScale(p.scale)
		dp.Positive().SetOffset(p.posOffset)
		dp.Positive().BucketCounts().FromRaw(s.flat[p.posStart:p.posEnd])
		dp.Negative().SetOffset(p.negOffset)
		dp.Negative().BucketCounts().FromRaw(s.flat[p.posEnd:p.negEnd])
		moveExemplars(p.hasExemplars, p.exemplars, timestamp, dp.Exemplars())
		p.attributes.CopyTo(dp.Attributes())
	}
}
