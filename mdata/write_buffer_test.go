package mdata

import (
	"testing"

	"gopkg.in/raintank/schema.v1"
)

func testAddAndGet(t *testing.T, testData, expectedData []schema.Point, expectAddFail bool) {
	b := NewWriteBuffer(60)
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
