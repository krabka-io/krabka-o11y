// Command tsdbdump prints every sample of a Prometheus TSDB block, after
// tombstones, as JSON. It is the reference decoder for the Krabka fixture.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"

	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type span struct {
	Offset int32  `json:"offset"`
	Length uint32 `json:"length"`
}

type hist struct {
	T             int64     `json:"t"`
	IsFloat       bool      `json:"is_float"`
	ResetHint     int       `json:"reset_hint"`
	Schema        int32     `json:"schema"`
	ZeroThreshold string    `json:"zero_threshold"`
	ZeroCount     string    `json:"zero_count"`
	Count         string    `json:"count"`
	Sum           string    `json:"sum"`
	PositiveSpans []span    `json:"positive_spans"`
	PositiveCounts []string `json:"positive_counts"`
	NegativeSpans []span    `json:"negative_spans"`
	NegativeCounts []string `json:"negative_counts"`
	CustomValues  []string  `json:"custom_values"`
}

type sample struct {
	T int64  `json:"t"`
	V string `json:"v"`
}

type series struct {
	Labels     map[string]string `json:"labels"`
	Floats     []sample          `json:"floats"`
	Histograms []hist            `json:"histograms"`
}

func bits(f float64) string { return fmt.Sprintf("%016x", math.Float64bits(f)) }

func spans(in []histogram.Span) []span {
	out := []span{}
	for _, s := range in {
		out = append(out, span{s.Offset, s.Length})
	}
	return out
}

func absolute(in []float64) []string {
	out := []string{}
	for _, v := range in {
		out = append(out, bits(v))
	}
	return out
}

func main() {
	b, err := tsdb.OpenBlock(promslog.NewNopLogger(), os.Args[1], nil, nil)
	if err != nil {
		panic(err)
	}
	q, err := tsdb.NewBlockQuerier(b, math.MinInt64, math.MaxInt64)
	if err != nil {
		panic(err)
	}
	ss := q.Select(context.Background(), true, nil, labels.MustNewMatcher(labels.MatchRegexp, "__name__", ".+"))
	out := []series{}
	for ss.Next() {
		s := ss.At()
		row := series{Labels: s.Labels().Map(), Floats: []sample{}, Histograms: []hist{}}
		it := s.Iterator(nil)
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			switch vt {
			case chunkenc.ValFloat:
				t, v := it.At()
				row.Floats = append(row.Floats, sample{t, bits(v)})
			case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
				t, fh := it.AtFloatHistogram(nil)
				row.Histograms = append(row.Histograms, hist{
					T: t, IsFloat: vt == chunkenc.ValFloatHistogram, ResetHint: int(fh.CounterResetHint),
					Schema: fh.Schema, ZeroThreshold: bits(fh.ZeroThreshold), ZeroCount: bits(fh.ZeroCount),
					Count: bits(fh.Count), Sum: bits(fh.Sum),
					PositiveSpans: spans(fh.PositiveSpans), PositiveCounts: absolute(fh.PositiveBuckets),
					NegativeSpans: spans(fh.NegativeSpans), NegativeCounts: absolute(fh.NegativeBuckets),
					CustomValues: absolute(fh.CustomValues),
				})
			}
		}
		if it.Err() != nil {
			panic(it.Err())
		}
		out = append(out, row)
	}
	if ss.Err() != nil {
		panic(ss.Err())
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", " ")
	if err := enc.Encode(out); err != nil {
		panic(err)
	}
}
