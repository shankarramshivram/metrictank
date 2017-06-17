package mdata

import (
	"sync"

	"gopkg.in/raintank/schema.v1"
)

var bufPool = sync.Pool{New: func() interface{} { return &entry{} }}

/*
 * The write buffer keeps a time-window of data during which it is ok to send data out of order.
 * Once the reorder window has passed it will try to flush the data out.
 * The write buffer itself is not thread safe because it is used by AggMetric, which is.
 */

type WriteBuffer struct {
	reorderWindow uint32 // how many datapoints are allowed to be out of order
	first         *entry // first buffer entry
	last          *entry // last buffer entry
	interval      uint32 // seconds per datapoint
	len           uint32
}

type entry struct {
	ts         uint32
	val        float64
	next, prev *entry
}

func NewWriteBuffer(reorderWindow, interval uint32) *WriteBuffer {
	return &WriteBuffer{
		reorderWindow: reorderWindow,
		interval:      interval,
	}
}

func (wb *WriteBuffer) Add(ts uint32, val float64) bool {
	// out of order and too old
	if wb.first != nil && ts < wb.first.ts {
		return false
	}

	e := bufPool.Get().(*entry)
	e.ts = ts
	e.val = val

	// initializing the linked list
	if wb.first == nil {
		wb.first = e
		wb.last = e
		wb.len++
		return true
	}

	// in the normal case data should be added in order, so this will only iterate once
	for i := wb.last; i != nil; i = i.prev {
		if ts > i.ts {
			if i.next != nil {
				e.next = i.next
				i.next.prev = e
			}
			e.prev = i
			i.next = e
			wb.len++

			return true
		}
		// overwrite and return
		if ts == i.ts {
			i.val = val
			return true
		}
	}

	// unlikely case where the added entry is the oldest one present
	wb.first.prev = e
	e.next = wb.first
	wb.first = e
	wb.len++
	return true
}

func (wb *WriteBuffer) Flush(now, upTo uint32, push func(uint32, float64)) {
	keepFrom := now - (wb.reorderWindow * wb.interval)
	if upTo > keepFrom {
		upTo = keepFrom
	}

	i := wb.first
	for {
		if i == nil || i.ts >= upTo {
			break
		}
		push(i.ts, i.val)
		wb.len--
		recycle := i
		i = i.next
		bufPool.Put(recycle)
	}

	if i.next == nil {
		wb.first = nil
		wb.last = nil
	} else {
		i.prev = nil
		wb.first = i
	}
}

func (wb *WriteBuffer) Get() []schema.Point {
	if wb.first == nil {
		return nil
	}

	res := make([]schema.Point, 0, wb.len)
	for i := wb.first; i.next != nil; i = i.next {
		res = append(res, schema.Point{Val: i.val, Ts: i.ts})
	}

	return res
}
