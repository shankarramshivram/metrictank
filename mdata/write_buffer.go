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
	reorderWindow uint32 // window size in datapoints during which out of order is allowed
	interval      uint32 // seconds per datapoint
	len           uint32
	flushMin      uint32 // min count of datapoints to flush
	first         *entry // first buffer entry
	last          *entry // last buffer entry
}

type entry struct {
	ts         uint32
	val        float64
	next, prev *entry
}

func NewWriteBuffer(reorderWindow, interval, flushMin uint32) *WriteBuffer {
	return &WriteBuffer{
		reorderWindow: reorderWindow,
		interval:      interval,
		// we don't want to flush unless we have at least flushMin + reorderWindow datapoints
		flushMin: flushMin + reorderWindow,
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
	addCount := uint32(0)

	// initializing the linked list
	if wb.first == nil {
		e.next = nil
		e.prev = nil
		wb.first = e
		wb.last = e
		addCount = 1
	} else {
		inserted := false
		// in the normal case data should be added in order, so this will only iterate once
		for i := wb.last; i != nil; i = i.prev {
			if ts > i.ts {
				if i.next == nil {
					e.next = nil
					wb.last = e
				} else {
					e.next = i.next
					e.next.prev = e
				}
				e.prev = i
				i.next = e
				addCount = 1
				inserted = true
				break
			}
			// overwrite and return
			if ts == i.ts {
				i.val = val
				inserted = true
				break
			}
		}
		if !inserted {
			// unlikely case where the added entry is the oldest one present
			e.prev = nil
			e.next = wb.first
			wb.first.prev = e
			wb.first = e
			wb.len += addCount
		}
	}

	return true
}

func (wb *WriteBuffer) Flush(now, upTo uint32, push func(uint32, float64)) {
	keepFrom := now - (wb.reorderWindow * wb.interval)
	if upTo == 0 || upTo > keepFrom {
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
	res := make([]schema.Point, 0, wb.len)
	if wb.first == nil {
		return res
	}

	for i := wb.first; i != nil; i = i.next {
		res = append(res, schema.Point{Val: i.val, Ts: i.ts})
	}

	return res
}
