package mdata

import (
	"sync"

	"github.com/dgryski/go-tsz"
	"github.com/raintank/metrictank/mdata/chunk"
)

var bufPool = sync.Pool{New: func() interface{} { return &entry{} }}

/*
 * The write buffer keeps a time-window of data during which it is ok to send data out of order.
 * Once the reorder window has passed it will try to flush the data out.
 * The write buffer itself is not thread safe because it is used by AggMetric, which is.
 */

type WriteBuffer struct {
	reorderWindow uint32      // how many datapoints are allowed to be out of order
	first         *entry      // first buffer entry
	last          *entry      // last buffer entry
	interval      uint32      // seconds per datapoint
	chunk         *tsz.Series // used to cache the return value of Get()
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

			if wb.chunk != nil {
				if wb.chunk.T0 < i.ts {
					wb.chunk.Push(i.ts, i.val)
				} else {
					wb.chunk = nil
				}
			}

			return true
		}
		// overwrite and return
		if ts == i.ts {
			return true
		}
	}

	// unlikely case where the added entry is the oldest one present
	wb.first.prev = e
	e.next = wb.first
	wb.first = e
	return true
}

func (wb *WriteBuffer) Flush(now, upTo uint32, push func(uint32, float64)) {
	keepFrom := now - (wb.reorderWindow * wb.interval)
	if upTo > keepFrom {
		upTo = keepFrom
	}

	if wb.first == nil || wb.first.ts >= upTo {
		return
	}
	i := wb.first
	wb.chunk = nil

	// process datapoints up to upTo timestamp
	for {
		push(i.ts, i.val)
		if i.next == nil || i.next.ts >= upTo {
			break
		}
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

func (wb *WriteBuffer) Get() *chunk.Iter {
	if wb.first == nil {
		return nil
	}

	if wb.chunk == nil {
		wb.chunk = tsz.New(wb.first.ts)
		for i := wb.first; i.next != nil; i = i.next {
			wb.chunk.Push(i.ts, i.val)
		}
	}

	it := chunk.NewIter(wb.chunk.Iter())
	return &it
}
