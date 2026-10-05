package main

import (
	"fmt"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func main() {
	c := chunkenc.NewXORChunk()
	a, _ := c.Appender()
	for i := int64(0); i < 3; i++ {
		a.Append(0, 1700000000000+i*1000, float64(7+i))
	}
	fmt.Printf("xor: %x\n", c.Bytes())
	c = chunkenc.NewXORChunk()
	a, _ = c.Appender()
	a.Append(0, 1700000001000, 8)
	fmt.Printf("xor_middle: %x\n", c.Bytes())
	for _, t := range []int64{1700000000000, 1700000001000} {
		h := &histogram.Histogram{CounterResetHint: histogram.GaugeType, Schema: 0, Count: 5, Sum: 8, PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}}, PositiveBuckets: []int64{2, 1}}
		c := chunkenc.NewHistogramChunk()
		a, _ := c.Appender()
		_, _, _, err := a.AppendHistogram(nil, 0, t, h, false)
		if err != nil {
			panic(err)
		}
		fmt.Printf("integer_%d: %x\n", t, c.Bytes())
		fh := h.ToFloat(nil)
		c2 := chunkenc.NewFloatHistogramChunk()
		a2, _ := c2.Appender()
		_, _, _, err = a2.AppendFloatHistogram(nil, 0, t, fh, false)
		if err != nil {
			panic(err)
		}
		fmt.Printf("float_%d: %x\n", t, c2.Bytes())
	}
}
