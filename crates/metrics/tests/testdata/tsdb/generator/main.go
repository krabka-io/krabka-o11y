// Command tsdbgen remote-writes the Krabka TSDB import fixture dataset into a
// Prometheus server. The dataset is deterministic: the same binary always
// sends the same samples.
package main

import (
	"bytes"
	"fmt"
	"math"
	"net/http"
	"os"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
)

const (
	t0       int64 = 1749996000000 // 2025-06-15T14:00:00Z, aligned to 2h.
	step     int64 = 15000
	nSamples       = 840 // 3.5h, so the head compacts the first 2h.
)

func lbls(kv ...string) []prompb.Label {
	out := []prompb.Label{}
	for i := 0; i < len(kv); i += 2 {
		out = append(out, prompb.Label{Name: kv[i], Value: kv[i+1]})
	}
	return out
}

// ts returns the timestamp of sample i with a deterministic jitter so the
// XOR chunks exercise every delta-of-delta width.
func ts(i int) int64 {
	t := t0 + int64(i)*step
	switch i % 7 {
	case 3:
		t += 37
	case 5:
		t -= 1200
	}
	if i%97 == 50 {
		t += 4000
	}
	return t
}

func main() {
	stale := math.Float64frombits(value.StaleNaN)
	var series []prompb.TimeSeries

	counter := prompb.TimeSeries{Labels: lbls("__name__", "imported_counter_total", "instance", "i1", "job", "fixture")}
	gauge := prompb.TimeSeries{Labels: lbls("__name__", "imported_gauge", "instance", "i1", "job", "fixture")}
	utf := prompb.TimeSeries{Labels: lbls("__name__", "imported_gauge", "instance", "zürich", "job", "fixture")}
	deleted := prompb.TimeSeries{Labels: lbls("__name__", "imported_deleted", "job", "fixture")}
	c := 0.0
	for i := 0; i < nSamples; i++ {
		t := ts(i)
		c += float64(i%11) + 0.25
		if i == 200 {
			c = 3 // counter reset
		}
		v := c
		if i == 300 {
			v = stale
		}
		counter.Samples = append(counter.Samples, prompb.Sample{Timestamp: t, Value: v})
		g := math.Sin(float64(i)/10)*1000 - 17.125
		if i%50 == 0 {
			g = -1e300
		}
		gauge.Samples = append(gauge.Samples, prompb.Sample{Timestamp: t, Value: g})
		utf.Samples = append(utf.Samples, prompb.Sample{Timestamp: t, Value: float64(i % 3)})
		deleted.Samples = append(deleted.Samples, prompb.Sample{Timestamp: t, Value: float64(i)})
	}
	series = append(series, counter, gauge, utf, deleted)

	hist := prompb.TimeSeries{Labels: lbls("__name__", "imported_hist", "job", "fixture")}
	var base uint64
	for i := 0; i < nSamples; i++ {
		t := ts(i)
		if i == 250 {
			base = 0 // counter reset
		}
		base++
		k := base
		h := &histogram.Histogram{
			Schema:          3,
			ZeroThreshold:   0.001,
			ZeroCount:       k,
			Count:           k*(1+2+3+4) + k*(1+1) + k,
			Sum:             float64(k) * 1.5,
			PositiveSpans:   []histogram.Span{{Offset: -2, Length: 2}, {Offset: 3, Length: 2}},
			PositiveBuckets: []int64{int64(k), int64(k), int64(k), int64(k)}, // absolute k,2k,3k,4k
			NegativeSpans:   []histogram.Span{{Offset: 1, Length: 2}},
			NegativeBuckets: []int64{int64(k), 0},
		}
		if i == 400 {
			h = &histogram.Histogram{Sum: stale}
		}
		hist.Histograms = append(hist.Histograms, prompb.FromIntHistogram(t, h))
	}
	series = append(series, hist)

	fh := prompb.TimeSeries{Labels: lbls("__name__", "imported_float_hist", "job", "fixture")}
	for i := 0; i < nSamples; i++ {
		t := ts(i)
		x := float64(i%13) + 0.5
		h := &histogram.FloatHistogram{
			CounterResetHint: histogram.GaugeType,
			Schema:           0,
			ZeroThreshold:    1e-128,
			ZeroCount:        x / 4,
			Count:            x/4 + x + x*2.5,
			Sum:              -x * 3.25,
			PositiveSpans:    []histogram.Span{{Offset: 0, Length: 2}},
			PositiveBuckets:  []float64{x, x * 2.5},
		}
		if i == 420 {
			h = &histogram.FloatHistogram{Sum: stale}
		}
		fh.Histograms = append(fh.Histograms, prompb.FromFloatHistogram(t, h))
	}
	series = append(series, fh)

	nhcb := prompb.TimeSeries{Labels: lbls("__name__", "imported_nhcb", "job", "fixture")}
	for i := 0; i < nSamples; i++ {
		t := ts(i)
		k := uint64(i + 1)
		h := &histogram.Histogram{
			Schema:          histogram.CustomBucketsSchema,
			Count:           k * 4,
			Sum:             float64(k) * 0.7,
			PositiveSpans:   []histogram.Span{{Offset: 0, Length: 4}},
			PositiveBuckets: []int64{int64(k), 0, 0, 0},
			CustomValues:    []float64{0.1, 0.5, 2.5, 1234.5678},
		}
		p := prompb.FromIntHistogram(t, h)
		p.CustomValues = h.CustomValues
		nhcb.Histograms = append(nhcb.Histograms, p)
	}
	series = append(series, nhcb)

	wr := prompb.WriteRequest{Timeseries: series}
	b, err := wr.Marshal()
	if err != nil {
		panic(err)
	}
	req, err := http.NewRequest(http.MethodPost, os.Args[1]+"/api/v1/write", bytes.NewReader(snappy.Encode(nil, b)))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	fmt.Println(resp.Status)
	if resp.StatusCode/100 != 2 {
		os.Exit(1)
	}
}
