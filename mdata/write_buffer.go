package mdata

import (
	"sync"

	"github.com/raintank/metrictank/conf"
	"gopkg.in/raintank/schema.v1"
)

var bufPool = sync.Pool{New: func() interface{} { return &entry{} }}

/*
 * The write buffer keeps a time-window of data during which it is ok to send data out of order.
 * Once the reorder window has passed it will try to flush the data out.
 * The write buffer itself is not thread safe because it is used by AggMetric, which is.
 */

type WriteBuffer struct {
	sync.RWMutex
	reorderWindow uint32 // window size in datapoints during which out of order is allowed
	interval      uint32 // seconds per datapoint
	len           uint32
	lastFlush     uint32                // the timestamp of the last point that's been flushed
	flushMin      uint32                // min count of datapoints to trigger a flush on
	first         *entry                // first buffer entry
	last          *entry                // last buffer entry
	flushStart    *entry                // entry from where a flush will start to walk the list
	flush         func(uint32, float64) //flushing function
}

type entry struct {
	ts         uint32
	val        float64
	next, prev *entry
}

func NewWriteBuffer(conf *conf.WriteBufferConf, interval uint32, flush func(uint32, float64)) *WriteBuffer {
	return &WriteBuffer{
		reorderWindow: conf.ReorderWindow,
		interval:      interval,
		flushMin:      conf.FlushMin,
		flush:         flush,
	}
}

func (wb *WriteBuffer) Add(ts uint32, val float64) bool {
	wb.Lock()
	defer wb.Unlock()

	// out of order and too old
	if ts < wb.lastFlush {
		return false
	}

	e := bufPool.Get().(*entry)
	e.ts = ts
	e.val = val

	// initializing the linked list
	if wb.first == nil {
		e.next = nil
		e.prev = nil
		wb.first = e
		wb.last = e
		wb.len++
	} else {
		// in the normal case data should be added in order, so this will only iterate once
		for i := wb.last; i != nil; i = i.prev {
			if ts > i.ts {
				if i.next == nil {
					wb.last = e
				} else {
					i.next.prev = e
				}
				e.next = i.next
				e.prev = i
				i.next = e
				e = nil
				wb.len++
				break
			}
			// overwrite value
			if ts == i.ts {
				i.val = val
				e = nil
				break
			}
		}
		if e != nil {
			// unlikely case where the added entry is the oldest one present
			e.prev = nil
			e.next = wb.first
			wb.first.prev = e
			wb.first = e
			wb.len++
		}
	}

	return true
}

// if buffer is ready for flushing, this will flush
func (wb *WriteBuffer) FlushIfReady() {
	wb.RLock()
	// not enough data, not ready to flush
	if wb.len < wb.flushMin {
		wb.RUnlock()
		return
	}

	// seek the entry up to which we'll want to flush
	flushEnd := wb.last
	for cnt := uint32(0); cnt < wb.reorderWindow; flushEnd = flushEnd.prev {
		cnt++
	}

	wb.RUnlock()

	// split the list at flushEnd and then flush the older part
	wb.Lock()
	defer wb.Unlock()

	for i := wb.first; i != flushEnd.next; i = i.next {
		wb.flush(i.ts, i.val)
		bufPool.Put(i)
	}

	wb.first = flushEnd.next
	wb.first.prev = nil
	wb.lastFlush = wb.first.ts
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
