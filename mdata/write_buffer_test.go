package mdata

import (
	"testing"

	"gopkg.in/raintank/schema.v1"
)

func testAddAndGet(t *testing.T, testData, expectedData []schema.Point, expectAddFail bool) {
	b := NewWriteBuffer(600, 1, 30, nil)
	gotFailure := false
	for _, point := range testData {
		success := b.Add(point.Ts, point.Val)
		if !success {
			gotFailure = true
		}
	}
	if expectAddFail && !gotFailure {
		t.Fatal("Expected an add to fail, but they all succeeded")
	}
	returned := b.Get()

	if len(expectedData) != len(returned) {
		t.Fatal("Length of returned and testData data unequal")
	}
	for i, _ := range expectedData {
		if expectedData[i] != returned[i] {
			t.Fatal("Returned data does not match testData data %v, %v", testData[i], returned[i])
		}
	}
}

func unsort(data []schema.Point, unsortBy int) []schema.Point {
	out := make([]schema.Point, len(data))
	i := 0
	for ; i < len(data)-unsortBy; i = i + unsortBy {
		for j := 0; j < unsortBy; j++ {
			out[i+j] = data[i+unsortBy-j-1]
		}
	}
	for ; i < len(data); i++ {
		out[i] = data[i]
	}
	return out
}

func TestUnsort(t *testing.T) {
	testData := []schema.Point{
		{Ts: 0, Val: 0},
		{Ts: 1, Val: 100},
		{Ts: 2, Val: 200},
		{Ts: 3, Val: 300},
		{Ts: 4, Val: 400},
		{Ts: 5, Val: 500},
		{Ts: 6, Val: 600},
		{Ts: 7, Val: 700},
		{Ts: 8, Val: 800},
		{Ts: 9, Val: 900},
	}
	expectedData := []schema.Point{
		{Ts: 2, Val: 200},
		{Ts: 1, Val: 100},
		{Ts: 0, Val: 0},
		{Ts: 5, Val: 500},
		{Ts: 4, Val: 400},
		{Ts: 3, Val: 300},
		{Ts: 8, Val: 800},
		{Ts: 7, Val: 700},
		{Ts: 6, Val: 600},
		{Ts: 9, Val: 900},
	}
	unsortedData := unsort(testData, 3)

	for i := 0; i < len(expectedData); i++ {
		if unsortedData[i] != expectedData[i] {
			t.Fatalf("unsort function returned unexpected data %+v", unsortedData)
		}
	}
}

func TestAddAndGetInOrder(t *testing.T) {
	testData := []schema.Point{
		{Ts: 1, Val: 100},
		{Ts: 2, Val: 200},
		{Ts: 3, Val: 300},
	}
	expectedData := []schema.Point{
		{Ts: 1, Val: 100},
		{Ts: 2, Val: 200},
		{Ts: 3, Val: 300},
	}
	testAddAndGet(t, testData, expectedData, false)
}

func TestAddAndGetInReverseOrder(t *testing.T) {
	testData := []schema.Point{
		{Ts: 3, Val: 300},
		{Ts: 2, Val: 200},
		{Ts: 1, Val: 100},
	}
	expectedData := []schema.Point{
		{Ts: 3, Val: 300},
	}
	testAddAndGet(t, testData, expectedData, true)
}

func TestAddAndGetInMessedUpOrder(t *testing.T) {
	testData := []schema.Point{
		{Ts: 1, Val: 100},
		{Ts: 2, Val: 200},
		{Ts: 4, Val: 400},
		{Ts: 3, Val: 300},
		{Ts: 5, Val: 500},
		{Ts: 6, Val: 600},
		{Ts: 8, Val: 800},
		{Ts: 7, Val: 700},
		{Ts: 9, Val: 900},
	}
	expectedData := []schema.Point{
		{Ts: 1, Val: 100},
		{Ts: 2, Val: 200},
		{Ts: 3, Val: 300},
		{Ts: 4, Val: 400},
		{Ts: 5, Val: 500},
		{Ts: 6, Val: 600},
		{Ts: 7, Val: 700},
		{Ts: 8, Val: 800},
		{Ts: 9, Val: 900},
	}
	testAddAndGet(t, testData, expectedData, false)
}

func TestFlushSortedData(t *testing.T) {
	resultI := 0
	results := make([]schema.Point, 400)
	receiver := func(ts uint32, val float64) {
		results[resultI] = schema.Point{Ts: ts, Val: val}
		resultI++
	}
	buf := NewWriteBuffer(600, 1, 30, receiver)
	for i := 100; i < 1100; i++ {
		buf.Add(uint32(i), float64(i))
	}

	buf.FlushIfReady()
	for i := 0; i < 400; i++ {
		if results[i].Ts != uint32(i+100) || results[i].Val != float64(i+100) {
			t.Fatalf("Unexpected results %+v", results)
		}
	}
}

func TestFlushUnsortedData(t *testing.T) {
	resultI := 0
	results := make([]schema.Point, 400)
	receiver := func(ts uint32, val float64) {
		results[resultI] = schema.Point{Ts: ts, Val: val}
		resultI++
	}
	buf := NewWriteBuffer(600, 1, 3, receiver)
	data := make([]schema.Point, 1000)
	for i := 0; i < 1000; i++ {
		data[i] = schema.Point{Ts: uint32(i + 100), Val: float64(i + 100)}
	}
	unsortedData := unsort(data, 10)
	for i := 0; i < len(data); i++ {
		buf.Add(unsortedData[i].Ts, unsortedData[i].Val)
	}
	buf.FlushIfReady()
	for i := 0; i < 400; i++ {
		if results[i].Ts != uint32(i+100) || results[i].Val != float64(i+100) {
			t.Fatalf("Unexpected results %+v", results)
		}
	}
}

func BenchmarkAddInOrder(b *testing.B) {
	data := make([]schema.Point, b.N)
	b.ResetTimer()

	buf := NewWriteBuffer(uint32(b.N), 1, 100, nil)
	for i := 0; i < b.N; i++ {
		buf.Add(data[i].Ts, data[i].Val)
	}
}

func BenchmarkAddOutOfOrder(b *testing.B) {
	data := make([]schema.Point, b.N)
	unsortedData := unsort(data, 10)
	b.ResetTimer()

	buf := NewWriteBuffer(uint32(b.N), 1, 100, nil)
	for i := 0; i < b.N; i++ {
		buf.Add(unsortedData[i].Ts, unsortedData[i].Val)
	}
}

/*func BenchmarkFlush(b *testing.B) {
	buffers := make([]*WriteBuffer, b.N)
	receiver := func(i uint32, j float64) {}
	for i := 0; i < b.N; i++ {
		for j := 0; j < 2000; j++ {
			buffers[i] = NewWriteBuffer(600, 1, 30, nil)
			buffers[i].Add(uint32(j), 1)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buffers[i].FlushIfReady()
	}
}*/
