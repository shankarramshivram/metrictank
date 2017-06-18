package mdata

import (
	"fmt"
	"testing"

	"gopkg.in/raintank/schema.v1"
)

func testAddAndGet(t *testing.T, testData, expectedData []schema.Point, expectAddFail bool) {
	b := NewWriteBuffer(600, 1, 30)
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

func TestFlush(t *testing.T) {
	buf := NewWriteBuffer(600, 1, 30)
	for i := 100; i < 1000; i++ {
		buf.Add(uint32(i), float64(i))
	}

	results := make([]schema.Point, 370)
	resultI := 0
	receiver := func(ts uint32, val float64) {
		results[resultI] = schema.Point{Ts: ts, Val: val}
		resultI++
	}
	buf.Flush(1001, 0, receiver)
	t.Fatalf(fmt.Sprintf("%d results: %+v", len(results), results))
}

func BenchmarkAddInOrder(b *testing.B) {
	buf := NewWriteBuffer(600, 1, 30)
	for i := 0; i < b.N; i++ {
		buf.Add(uint32(i), 1)
	}
}

func BenchmarkFlush(b *testing.B) {
	buffers := make([]*WriteBuffer, b.N)
	receiver := func(i uint32, j float64) {}
	for i := 0; i < b.N; i++ {
		for j := 0; j < 2000; j++ {
			buffers[i] = NewWriteBuffer(600, 1, 30)
			buffers[i].Add(uint32(j), 1)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buffers[i].Flush(uint32(b.N)+1, 0, receiver)
	}
}
