package bufq

import (
	"fmt"
	"math/bits"
	"path/filepath"
	"runtime"
	"sync"
)

type (
	Queue struct {
		mu   sync.Mutex
		cond sync.Cond

		q      []slot
		qr, qw int64

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
	// Queue.Msg can be used to wrap it to buffer index.
	FlagFullMsg = 1 << iota
)

var (
	ErrClosed     Error = Closed
	ErrWouldBlock Error = WouldBlock
)

func New(n, buf int) *Queue {
	q := &Queue{}
	q.Reset(n, buf)

	return q
}

func (q *Queue) ResetSame() {
	q.Reset(len(q.q), int(q.b))
}

func (q *Queue) Reset(n, buf int) {
	if n&0x3 != 0 {
		panic(n)
	}
	if buf&0xf != 0 {
		panic(buf)
	}

	defer q.mu.Unlock()
	q.mu.Lock()

	q.cond.L = &q.mu

	if n > cap(q.q) {
		q.q = make([]slot, n)
	} else {
		q.q = q.q[:n]
	}

	q.qr, q.qw = 0, 0

	q.b = int64(buf)
	q.r, q.w = 0, 0

	q.closed = false
}

func (q *Queue) Allocate(size, align int, blocking bool) (msg int64, st, end int) {
	if align > 0 {
		align = alignAlign(align)
	}

	defer q.mu.Unlock()
	q.mu.Lock()

	return q.allocate(size, align, blocking)
}

func (q *Queue) AllocateN(size, align int, blocking bool, buf []Message) (n int) {
	if align > 0 {
		align = alignAlign(align)
	}

	defer q.mu.Unlock()
	q.mu.Lock()

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

func (q *Queue) Commit(msg int64, size int) {
	defer q.mu.Unlock()
	q.mu.Lock()

	q.commit(msg, size)
}

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

func (q *Queue) Consume(blocking bool) (msg int64, st, end int) {
	defer q.mu.Unlock()
	q.mu.Lock()

	for {
		for msg := q.qr; msg < q.qw; msg++ {
			s := q.q[q.Msg(msg)]
			if s.size < 0 {
				continue
			}

			st := q.start(s.start)
			end := st + s.size

			q.q[q.Msg(msg)].size = sizeConsuming

			return q.msg(msg), st, end
		}

		if q.qr == q.qw && q.closed {
			return Closed, 0, 0
		}

		if !blocking {
			return WouldBlock, 0, 0
		}

		q.cond.Wait()
	}
}

func (q *Queue) ConsumeN(blocking bool, buf []Message) (n int) {
	defer q.mu.Unlock()
	q.mu.Lock()

	for {
		if q.closed && q.qr == q.qw {
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
	for msg := q.qr; n < len(buf) && msg < q.qw; msg++ {
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

func (q *Queue) Done(msg int64) {
	defer q.mu.Unlock()
	q.mu.Lock()

	s := q.q[q.Msg(msg)]
	if s.size != sizeConsuming && s.size != Cancel {
		panic("bufq: Queue misuse: done message which wasn't consumed")
	}

	q.q[q.Msg(msg)].size = sizeFree

	q.done()
}

func (q *Queue) DoneN(ms []Message) {
	if len(ms) == 0 {
		return
	}

	defer q.mu.Unlock()
	q.mu.Lock()

	for _, m := range ms {
		s := q.q[q.Msg(m.Msg)]
		if s.size != sizeConsuming && s.size != Cancel {
			panic("bufq: Queue misuse: done message which wasn't consumed")
		}

		q.q[q.Msg(m.Msg)].size = sizeFree
	}

	q.done()
}

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

func (q *Queue) Close() error {
	defer q.mu.Unlock()
	q.mu.Lock()

	q.closed = true

	q.cond.Broadcast()

	return nil
}

func (q *Queue) Size() (n, buf int) {
	return len(q.q), int(q.b)
}

func (q *Queue) len() int {
	defer q.mu.Unlock()
	q.mu.Lock()

	n := 0

	for msg := q.qr; msg < q.qw; msg++ {
		s := q.q[q.Msg(msg)]

		if s.size >= 0 {
			n++
		}
	}

	return n
}

func (q *Queue) Stats() (qr, qw, r, w int64) {
	defer q.mu.Unlock()
	q.mu.Lock()

	return q.qr, q.qw, q.r, q.w
}

func (q *Queue) state() (x uint64) {
	for msg := q.qr; msg < q.qw; msg++ {
		m := q.Msg(msg)
		s := q.q[m]

		sh := 4 * int(msg-q.qr)
		if sh >= 64 {
			break
		}

		switch {
		case s.size == sizeFree:
			// x |= 0 << sh
		case s.size == sizeAllocated:
			x |= 1 << sh
		case s.size >= 0:
			x |= 2 << sh
		case s.size == sizeConsuming:
			x |= 3 << sh
		case s.size == Cancel:
			x |= 4 << sh
		default:
			panic(s.size)
		}
	}

	return x
}

func (q *Queue) Msg(msg int64) int64 { return msg % q.qlen() }
func (q *Queue) msg(msg int64) int64 {
	if q.Flags&(1<<FlagFullMsg) != 0 {
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

func (q *Queue) pState() string {
	return fmt.Sprintf("q %3x-%3x  b %4x-%4x  s %x", q.qr, q.qw, q.r, q.w, q.state())
}

/*
func (q *Queue) String() string {
	defer q.mu.Unlock()
	q.mu.Lock()

	return q.pState()
}

func (q *Queue) dump() string {
	defer q.mu.Unlock()
	q.mu.Lock()

	var b strings.Builder

	for msg := q.qr; msg < q.qw; msg++ {
		s := q.q[q.Msg(msg)]

		fmt.Fprintf(&b, "msg %4x: st %4x  size %3x\n", msg, s.start, s.size)
	}

	return b.String()
}
*/

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

func alignAlign(align int) int {
	return 1 << bits.Len(uint(align)-1)
}

func caller(d int) string {
	_, file, line, _ := runtime.Caller(1 + d)

	return fmt.Sprintf("%v:%v", filepath.Base(file), line)
}
