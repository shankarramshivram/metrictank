package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func TestDerivativeNoMax(t *testing.T) {
	testDerivative(
		"derivative",
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "abcd",
				Target:     "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "abcd",
				Target:     "b",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				QueryPatt:  "abcd",
				Target:     "c",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				QueryPatt:  "abcd",
				Target:     "d",
				Datapoints: getCopy(d),
			},
		},
		[]models.Series{
			{
				Interval:  10,
				QueryPatt: "derivative(abcd)",
				Datapoints: []schema.Point{
					{Val: math.NaN(), Ts: 10},
					{Val: 0, Ts: 20},
					{Val: 5.5, Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: math.NaN(), Ts: 50},
					{Val: math.NaN(), Ts: 60},
				},
			},
			{
				Interval:  10,
				QueryPatt: "derivative(abcd)",
				Datapoints: []schema.Point{
					{Val: math.NaN(), Ts: 10},
					{Val: math.MaxFloat64, Ts: 20},
					{Val: 0, Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: math.NaN(), Ts: 50},
					{Val: math.NaN(), Ts: 60},
				},
			},
			{
				Interval:  10,
				QueryPatt: "derivative(abcd)",
				Datapoints: []schema.Point{
					{Val: math.NaN(), Ts: 10},
					{Val: 0, Ts: 20},
					{Val: 1, Ts: 30},
					{Val: 1, Ts: 40},
					{Val: 1, Ts: 50},
					{Val: 1, Ts: 60},
				},
			},
			{
				Interval:  10,
				QueryPatt: "derivative(abcd)",
				Datapoints: []schema.Point{
					{Val: math.NaN(), Ts: 10},
					{Val: 33, Ts: 20},
					{Val: 166, Ts: 30},
					{Val: -170, Ts: 40},
					{Val: 51, Ts: 50},
					{Val: 170, Ts: 60},
				},
			},
		},
		t,
	)
}

func testDerivative(name string, in []models.Series, out []models.Series, t *testing.T) {
	f := NewDerivative()
	f.(*FuncDerivative).in = NewMock(in)
	gots, err := f.Exec(make(map[Req][]models.Series))
	if err != nil {
		t.Fatalf("case %q: err should be nil. got %q", name, err)
	}
	if len(gots) != len(out) {
		t.Fatalf("case %q: isNonNull len output expected %d, got %d", name, len(out), len(gots))
	}
	for i, g := range gots {
		exp := out[i]
		if g.QueryPatt != exp.QueryPatt {
			t.Fatalf("case %q: expected target %q, got %q", name, exp.QueryPatt, g.QueryPatt)
		}
		if len(g.Datapoints) != len(exp.Datapoints) {
			t.Fatalf("case %q len output expected %d, got %d", name, len(exp.Datapoints), len(g.Datapoints))
		}
		for j, p := range g.Datapoints {
			bothNaN := math.IsNaN(p.Val) && math.IsNaN(exp.Datapoints[j].Val)
			if (bothNaN || p.Val == exp.Datapoints[j].Val) && p.Ts == exp.Datapoints[j].Ts {
				continue
			}
			t.Fatalf("case %q: output point %d - expected %v got %v", name, j, exp.Datapoints[j], p)
		}
	}
}
func BenchmarkDerivative10k_1NoNulls(b *testing.B) {
	benchmarkDerivative(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkDerivative10k_10NoNulls(b *testing.B) {
	benchmarkDerivative(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkDerivative10k_100NoNulls(b *testing.B) {
	benchmarkDerivative(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkDerivative10k_1000NoNulls(b *testing.B) {
	benchmarkDerivative(b, 1000, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkDerivative10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkDerivative(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkDerivative10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkDerivative(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkDerivative10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkDerivative(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkDerivative10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkDerivative(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkDerivative10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkDerivative(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkDerivative10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkDerivative(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkDerivative10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkDerivative(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkDerivative10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkDerivative(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func benchmarkDerivative(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
	var input []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			QueryPatt: strconv.Itoa(i),
		}
		if i%2 == 0 {
			series.Datapoints = fn0()
		} else {
			series.Datapoints = fn1()
		}
		input = append(input, series)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := NewDerivative()
		f.(*FuncDerivative).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
