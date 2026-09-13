// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spanmetricsconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/spanmetricsconnector"

import (
	"bytes"
	"slices"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/otel/semconv/v1.40.0"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/spanmetricsconnector/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/spanmetricsconnector/internal/metrics"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"
)

// SPIKE (option A): pre-aggregate one request outside p.lock, then apply the result under the lock.
// Exemplars and events are not supported; ConsumeTraces falls back to the original path for them.
var spikePreAgg bool

// preAggSeries holds what is needed to create a series (first span seen for the key) plus
// the values accumulated for it within the request.
type preAggSeries struct {
	serviceName string
	span        ptrace.Span
	resource    pcommon.Map
	scope       pcommon.InstrumentationScope
	isAdjusted  bool
	count       uint64       // calls
	obs         []preAggObs  // duration observations
}

type preAggObs struct {
	value float64
	n     uint64
}

type preAggResource struct {
	key       resourceKey
	attr      pcommon.Map // attributes of the first ResourceSpans seen with this key
	sums      map[metrics.Key]*preAggSeries
	sumOrder  []metrics.Key
	hists     map[metrics.Key]*preAggSeries
	histOrder []metrics.Key
}

func (p *connectorImp) consumeTracesPreAgg(traces ptrace.Traces) error {
	batch := p.preAggregate(traces)
	p.lock.Lock()
	p.applyPreAggregated(batch)
	p.lock.Unlock()
	return nil
}

// preAggregate runs without p.lock. It only reads immutable connector state.
func (p *connectorImp) preAggregate(traces ptrace.Traces) []*preAggResource {
	var buf bytes.Buffer
	adjustedCountCache := metrics.NewAdjustedCountCache()
	unitDiv := unitDivider(p.config.Histogram.Unit)
	byKey := make(map[resourceKey]*preAggResource)
	var out []*preAggResource

	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		rspans := traces.ResourceSpans().At(i)
		resourceAttr := rspans.Resource().Attributes()
		serviceAttr, ok := resourceAttr.Get(string(conventions.ServiceNameKey))
		if !ok {
			continue
		}
		rk := p.createResourceKey(resourceAttr)
		r := byKey[rk]
		if r == nil {
			r = &preAggResource{
				key:   rk,
				attr:  resourceAttr,
				sums:  make(map[metrics.Key]*preAggSeries),
				hists: make(map[metrics.Key]*preAggSeries),
			}
			byKey[rk] = r
			out = append(out, r)
		}

		serviceName := serviceAttr.Str()
		ilsSlice := rspans.ScopeSpans()
		for j := 0; j < ilsSlice.Len(); j++ {
			ils := ilsSlice.At(j)
			spans := ils.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				duration := float64(0)
				if span.EndTimestamp() > span.StartTimestamp() {
					duration = float64(span.EndTimestamp()-span.StartTimestamp()) / float64(unitDiv)
				}
				adjustedCount, isAdjusted := metrics.GetStochasticAdjustedCountWithCache(&span, &adjustedCountCache)

				key := p.buildKeyWith(&buf, serviceName, span, p.dimensions, p.callsDimensions, resourceAttr, isAdjusted)
				s := r.sums[key]
				if s == nil {
					s = &preAggSeries{serviceName: serviceName, span: span, resource: resourceAttr, scope: ils.Scope(), isAdjusted: isAdjusted}
					r.sums[key] = s
					r.sumOrder = append(r.sumOrder, key)
				}
				s.count += adjustedCount

				if !p.config.Histogram.Disable {
					dk := p.buildKeyWith(&buf, serviceName, span, p.dimensions, p.durationDimensions, resourceAttr, isAdjusted)
					h := r.hists[dk]
					if h == nil {
						h = &preAggSeries{serviceName: serviceName, span: span, resource: resourceAttr, scope: ils.Scope(), isAdjusted: isAdjusted}
						r.hists[dk] = h
						r.histOrder = append(r.histOrder, dk)
					}
					h.obs = append(h.obs, preAggObs{value: duration, n: adjustedCount})
				}
			}
		}
	}
	return out
}

// applyPreAggregated must be called with p.lock held.
func (p *connectorImp) applyPreAggregated(batch []*preAggResource) {
	startTimestamp := pcommon.NewTimestampFromTime(p.clock.Now())
	lastSeen := p.clock.Now()
	for _, r := range batch {
		rm := p.getOrCreateResourceMetricsByKey(r.key, r.attr)
		for _, k := range r.sumOrder {
			s := r.sums[k]
			sum, _ := rm.sums.GetOrCreate(k, func() pcommon.Map {
				return p.buildAttributes(s.serviceName, s.span, s.resource, p.dimensions, p.callsDimensions, s.scope, s.isAdjusted)
			}, startTimestamp, lastSeen)
			sum.Add(s.count)
		}
		if p.config.Histogram.Disable {
			continue
		}
		for _, k := range r.histOrder {
			h := r.hists[k]
			hist, _ := rm.histograms.GetOrCreate(k, func() pcommon.Map {
				return p.buildAttributes(h.serviceName, h.span, h.resource, p.dimensions, p.durationDimensions, h.scope, h.isAdjusted)
			}, startTimestamp, lastSeen)
			for _, o := range h.obs {
				hist.ObserveN(o.value, o.n)
			}
		}
	}
}

// getOrCreateResourceMetricsByKey is getOrCreateResourceMetrics with a precomputed key.
func (p *connectorImp) getOrCreateResourceMetricsByKey(key resourceKey, attr pcommon.Map) *resourceMetrics {
	v, ok := p.resourceMetrics.Get(key)
	if !ok {
		v = &resourceMetrics{
			histograms: initHistogramMetrics(p.config),
			sums:       metrics.NewSumMetrics(p.config.Exemplars.MaxPerDataPoint, p.config.AggregationCardinalityLimit),
			events:     metrics.NewSumMetrics(p.config.Exemplars.MaxPerDataPoint, p.config.AggregationCardinalityLimit),
			attributes: attr,
		}
		p.resourceMetrics.Add(key, v)
	}
	if p.config.MetricsExpiration > 0 {
		v.lastSeen = p.clock.Now()
	}
	return v
}

// buildKeyWith is buildKey writing into a caller-owned buffer instead of the shared p.keyBuf.
func (p *connectorImp) buildKeyWith(buf *bytes.Buffer, serviceName string, span ptrace.Span, dimensions, optionalDims dimensionList, resourceOrEventAttrs pcommon.Map, isAdjusted bool) metrics.Key {
	buf.Reset()

	if !slices.Contains(p.config.ExcludeDimensions, serviceNameKey) {
		concatDimensionValue(buf, serviceName, false)
	}
	if !slices.Contains(p.config.ExcludeDimensions, spanNameKey) {
		concatDimensionValue(buf, span.Name(), true)
	}
	if !slices.Contains(p.config.ExcludeDimensions, spanKindKey) {
		concatDimensionValue(buf, traceutil.SpanKindStr(span.Kind()), true)
	}
	if metadata.SpanmetricsStatusCodeConventionUseOtelPrefixFeatureGate.IsEnabled() {
		if !slices.Contains(p.config.ExcludeDimensions, otelStatusCodeKey) {
			concatDimensionValue(buf, traceutil.StatusCodeStr(span.Status().Code()), true)
		}
	} else {
		if !slices.Contains(p.config.ExcludeDimensions, statusCodeKey) {
			concatDimensionValue(buf, traceutil.StatusCodeStr(span.Status().Code()), true)
		}
	}

	matchDimensions(dimensions, span, resourceOrEventAttrs, func(n string, v pcommon.Value) {
		concatDimensionValue(buf, n+":"+v.AsString(), true)
	})
	matchDimensions(optionalDims, span, resourceOrEventAttrs, func(n string, v pcommon.Value) {
		concatDimensionValue(buf, n+":"+v.AsString(), true)
	})

	if p.config.EnableMetricsSamplingMethod {
		if isAdjusted {
			concatDimensionValue(buf, "extrapolated", true)
		} else {
			concatDimensionValue(buf, "counted", true)
		}
	}

	return metrics.Key(buf.String())
}
