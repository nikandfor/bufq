// Package bufq is a queue of chunks of a shared ring buffer, passed by indexes.
package bufq

import (
	"fmt"
	"math/bits"
	"sync"
)

type (
	Queue struct {
		mu   sync.Mutex
		cond sync.Cond

		q      []slot
		qr, qw int64
		qc     int64 // first slot that can still become consumable

		b    int64
		r, w int64

		closed bool

		Flags Flags
	}

	slot struct {
		start int64
		size  int
	}

	Message struct {
		Msg int64

		Start int
		Size  int
	}

	Flags int
	Error int
)

// Special msg values returned by Allocate and Consume meaning errors.
const (
	Closed = -1 - iota
	WouldBlock
)

const (
	// Special size value passed to Commit and CommitN functions.
	// Canceled message is skipped by consumers and returns to free list.
	Cancel = -1

	// slot states
	sizeAllocated = Cancel - iota
	// sizeCommitted = positive size or Cancel
	sizeConsuming
	sizeFree
)

const (
	// FlagFullMsg makes Allocate to return always increasing message number.
	// Queue.Msg can be used to wrap it to queue index.
	FlagFullMsg Flags = 1 << iota
)

var (
	ErrClosed     Error = Closed
	ErrWouldBlock Error = WouldBlock
)

// New makes a queue of n messages over a buf bytes ring buffer.
// buf == 0 makes a metadata only queue: all sizes must be 0.
func New(n, buf int) *Queue {
	q := &Queue{}
	q.ResetSize(n, buf)

	return q
}

// Reset restarts the queue keeping its size. Messages in flight are dropped.
// All the users must be stopped first and must not reuse their msg values.
func (q *Queue) Reset() {
	q.ResetSize(len(q.q), int(q.b))
}

// ResetSize restarts the queue with a new size. Same access rules as Reset.
func (q *Queue) ResetSize(n, buf int) {
	if n < 0x4 || n&0x3 != 0 {
		panic(n)
	}
	if buf < 0 || buf&0xf != 0 {
		panic(buf)
	}

	q.cond.L = &q.mu

	if n > cap(q.q) {
		q.q = make([]slot, n)
	} else {
		q.q = q.q[:n]
	}

	q.qr, q.qw = 0, 0
	q.qc = 0

	q.b = int64(buf)
	q.r, q.w = 0, 0

	q.closed = false
}

// Allocate reserves a message and size bytes at st..end, aligned to align.
// msg is negative on error. Each message must be Committed.
func (q *Queue) Allocate(size, align int, blocking bool) (msg int64, st, end int) {
	defer q.mu.Unlock()
	q.mu.Lock()

	align = alignAlign(align)

	return q.allocate(size, align, blocking)
}

// AllocateN fills buf with up to len(buf) messages. It only blocks for the first one.
// n is negative on error.
func (q *Queue) AllocateN(size, align int, blocking bool, buf []Message) (n int) {
	defer q.mu.Unlock()
	q.mu.Lock()

	align = alignAlign(align)

	for n < len(buf) {
		msg, st, end := q.allocate(size, align, blocking && n == 0)
		if msg < 0 && n > 0 {
			return n
		}
		if msg < 0 {
			return int(msg)
		}

		buf[n] = Message{
			Msg:   msg,
			Start: st,
			Size:  end - st,
		}

		n++
	}

	return n
}

func (q *Queue) allocate(size, align int, blocking bool) (msg int64, st, end int) {
	//	defer func() {
	//		log.Printf("allocate %5v -> %3x  from %v %v %v", blocking, msg, caller(1), caller(2), caller(3))
	//	}()
	if size < 0 || int64(size) > q.b || size != 0 && q.b == 0 {
		panic(size)
	}
	if align < 0 || align != 0 && q.b%int64(align) != 0 {
		panic(align)
	}

	for {
		if q.closed {
			return Closed, 0, 0
		}

		if a := int64(align); a != 0 && q.w%a != 0 {
			q.w = q.w - q.w%a + a
		}

		if q.b != 0 && q.w%q.b+int64(size) > q.b {
			q.w = q.w - q.w%q.b + q.b
		}

		if q.qw+1 > q.qr+q.qlen() || q.w+int64(size) > q.r+q.b {
			if !blocking {
				return WouldBlock, 0, 0
			}

			q.cond.Wait()

			continue
		}

		msg := q.qw
		q.qw++

		q.q[q.Msg(msg)] = slot{start: q.w, size: sizeAllocated}

		st := q.start(q.w)
		end := st + size

		q.w += int64(size)

		return q.msg(msg), st, end
	}
}

// Commit publishes the first size bytes of the message to consumers.
// Cancel drops it instead.
func (q *Queue) Commit(msg int64, size int) {
	defer q.mu.Unlock()
	q.mu.Lock()

	q.commit(msg, size)
}

// CommitN commits messages using their Size field.
func (q *Queue) CommitN(ms []Message) {
	defer q.mu.Unlock()
	q.mu.Lock()

	for _, m := range ms {
		q.commit(m.Msg, m.Size)
	}
}

func (q *Queue) commit(msg int64, size int) {
	if size < 0 {
		size = Cancel
	}

	if q.q[q.Msg(msg)].size != sizeAllocated {
		panic("bufq: Queue misuse: message commit wasn't expected")
	}

	if size > 0 && q.b != 0 {
		cur := q.q[q.Msg(msg)]
		next := q.q[q.Msg(msg+1)]

		end := cur.start + int64(size)

		if eq := q.equal(msg+1, q.qw); !eq && end > next.start || eq && end > q.w {
			panic("bufq: Queue misuse: committed size is out of bounds")
		}
	}

	q.q[q.Msg(msg)].size = size

	if size == Cancel {
		q.done()
	} else {
		q.cond.Broadcast()
	}
}

// Consume takes a committed message. msg is negative on error.
// Each message must be Done.
func (q *Queue) Consume(blocking bool) (msg int64, st, end int) {
	defer q.mu.Unlock()
	q.mu.Lock()

	for {
		q.skipConsumed()

		for msg := q.qc; msg < q.qw; msg++ {
			s := q.q[q.Msg(msg)]
			if s.size < 0 {
				continue
			}

			st := q.start(s.start)
			end := st + s.size

			q.q[q.Msg(msg)].size = sizeConsuming

			return q.msg(msg), st, end
		}

		if q.qc == q.qw && q.closed {
			return Closed, 0, 0
		}

		if !blocking {
			return WouldBlock, 0, 0
		}

		q.cond.Wait()
	}
}

// ConsumeN fills buf with up to len(buf) committed messages. n is negative on error.
func (q *Queue) ConsumeN(blocking bool, buf []Message) (n int) {
	if len(buf) == 0 {
		return 0
	}

	defer q.mu.Unlock()
	q.mu.Lock()

	for {
		q.skipConsumed()

		if q.closed && q.qc == q.qw {
			return Closed
		}

		n = q.consumeN(buf)
		if n > 0 {
			return n
		}
		if !blocking {
			return WouldBlock
		}

		q.cond.Wait()
	}
}

func (q *Queue) consumeN(buf []Message) (n int) {
	for msg := q.qc; n < len(buf) && msg < q.qw; msg++ {
		s := q.q[q.Msg(msg)]
		if s.size < 0 {
			continue
		}

		st := q.start(s.start)
		end := st + s.size

		q.q[q.Msg(msg)].size = sizeConsuming

		buf[n] = Message{
			Msg:   q.msg(msg),
			Start: st,
			Size:  end - st,
		}

		n++
	}

	return n
}

// skipConsumed moves qc over the slots that can't become consumable again.
func (q *Queue) skipConsumed() {
	q.qc = max(q.qr, q.qc)

	for q.qc < q.qw {
		s := q.q[q.Msg(q.qc)]
		if s.size == sizeAllocated || s.size >= 0 {
			break
		}

		q.qc++
	}
}

// Done returns the message buffer to producers.
func (q *Queue) Done(msg int64) {
	defer q.mu.Unlock()
	q.mu.Lock()

	s := q.q[q.Msg(msg)]
	if s.size != sizeConsuming {
		panic("bufq: Queue misuse: done message which wasn't consumed")
	}

	q.q[q.Msg(msg)].size = sizeFree

	q.done()
}

// DoneN returns the message buffers to producers.
func (q *Queue) DoneN(ms []Message) {
	if len(ms) == 0 {
		return
	}

	defer q.mu.Unlock()
	q.mu.Lock()

	for _, m := range ms {
		s := q.q[q.Msg(m.Msg)]
		if s.size != sizeConsuming {
			panic("bufq: Queue misuse: done message which wasn't consumed")
		}

		q.q[q.Msg(m.Msg)].size = sizeFree
	}

	q.done()
}

// done moves qr over the retired slots and frees their buffer space.
func (q *Queue) done() {
	var moved bool

	for q.qr < q.qw {
		msg := q.Msg(q.qr)
		size := q.q[msg].size

		if size == Cancel || size == sizeFree {
			q.qr++
			moved = true
			continue
		}

		break
	}

	if !moved {
		return
	}

	if q.qr == q.qw {
		q.r = q.w
	} else {
		msg := q.Msg(q.qr)
		q.r = q.q[msg].start
	}

	q.cond.Broadcast()
}

// Close wakes up all waiters. Messages already in the queue are still consumable.
func (q *Queue) Close() error {
	defer q.mu.Unlock()
	q.mu.Lock()

	q.closed = true

	q.cond.Broadcast()

	return nil
}

// Size returns the sizes the queue was created with.
func (q *Queue) Size() (n, buf int) {
	return len(q.q), int(q.b)
}

// Stats returns the message and the buffer read/write positions.
func (q *Queue) Stats() (qr, qw, r, w int64) {
	defer q.mu.Unlock()
	q.mu.Lock()

	return q.qr, q.qw, q.r, q.w
}

// Msg wraps a FlagFullMsg message number to a queue index.
func (q *Queue) Msg(msg int64) int64 { return msg % q.qlen() }

func (q *Queue) msg(msg int64) int64 {
	if q.Flags.Is(FlagFullMsg) {
		return msg
	}

	return q.Msg(msg)
}

func (q *Queue) qlen() int64 { return int64(len(q.q)) }
func (q *Queue) start(st int64) int {
	if q.b == 0 {
		return 0
	}

	return int(st % q.b)
}

func (q *Queue) equal(x, y int64) bool { return q.Msg(x) == q.Msg(y) }

func (m Message) End() int             { return m.Start + m.Size }
func (m Message) StartEnd() (int, int) { return m.Start, m.Start + m.Size }
func (m *Message) Cancel()             { m.Size = Cancel }

func (e Error) Error() string {
	switch e {
	case Closed:
		return "queue is closed"
	case WouldBlock:
		return "would block"
	default:
		return fmt.Sprintf("unknown error: %d", int(e))
	}
}

func (f Flags) Is(v Flags) bool { return f&v == v }
func (f *Flags) Set(v Flags)    { *f |= v }
func (f *Flags) Unset(v Flags)  { *f &^= v }

func alignAlign(align int) int {
	if align <= 0 {
		return align
	}

	return 1 << bits.Len(uint(align)-1)
}
