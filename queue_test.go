package bufq_test

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"nikand.dev/go/bufq"
)

func TestQueue(tb *testing.T) {
	const N = 5

	meta := make([]int, 7*4)
	ms := make([]bufq.Message, 2)

	q := bufq.New(len(meta), 0)

	for i := range N {
		m := q.AllocateN(0, 0, false, ms)

		for j := range ms[:m] {
			msg := ms[j].Msg
			meta[msg] = (len(ms)*i + j) * 7

			tb.Logf("set msg %v  set %v", msg, meta[msg])
		}

		q.CommitN(ms[:m])
	}

	_ = q.Close()

	consumed := 0

	for {
		m := q.ConsumeN(false, ms)
		if m == bufq.Closed {
			break
		}
		if m < 0 {
			tb.Errorf("consume: %v", bufq.Error(m))
			break
		}

		for j := range ms[:m] {
			msg := ms[j].Msg

			tb.Logf("got msg %v  set %v", msg, meta[msg])

			if meta[msg] != consumed*7 {
				tb.Errorf("wanted %v, got %v", consumed, meta[msg])
			}

			consumed++
		}

		q.DoneN(ms[:m])
	}

	if consumed != len(ms)*N {
		tb.Errorf("wanted %v, got %v", len(ms)*N, consumed)
	}
}

func TestQueueParallel(tb *testing.T) {
	const Q, N, T = 8, 10, 1024

	var wg, wwg sync.WaitGroup
	var counter atomic.Int32

	read := make([]byte, T)

	meta := make([]int, Q)
	b := make([]byte, 12*Q)

	q := bufq.New(len(meta), len(b))

	q.Flags.Set(bufq.FlagFullMsg)

	wg.Add(N)
	wwg.Add(N)

	for i := range N {
		go func() {
			defer wg.Done()
			defer wwg.Done()

			for {
				x := counter.Add(1) - 1
				if x >= T {
					break
				}

				msg, st, _ := q.Allocate(15, 16, true)
				if msg < 0 {
					tb.Errorf("allocate: %x", msg)
					break
				}

				res := fmt.Appendf(b[:st], "%04x s_%02x_%03x", x, i, msg)
				size := len(res) - st

				meta[q.Msg(msg)] = i

				runtime.Gosched()

				q.Commit(msg, size)

				runtime.Gosched()
			}
		}()
	}

	wg.Add(N)
	wwg.Add(N)

	for i := range N {
		go func() {
			defer wg.Done()
			defer wwg.Done()

			ms := make([]bufq.Message, 3)

			for {
				m := q.AllocateN(15, 16, true, ms)
				if m == bufq.Closed {
					break
				}
				if m < 0 {
					tb.Errorf("allocate: %x", m)
					break
				}

				first := int(counter.Add(int32(m))) - m
				x := first

				for ; x < T && x < first+m; x++ {
					mm := &ms[x-first]

					res := fmt.Appendf(b[:mm.Start], "%04x N_%02x_%03x", x, i, mm.Msg)

					mm.Size = len(res) - mm.Start

					meta[q.Msg(mm.Msg)] = 0x1000 + i
				}
				for j := x; j < first+m; j++ {
					ms[j-first].Cancel()
				}

				runtime.Gosched()

				q.CommitN(ms[:m])

				runtime.Gosched()

				if first+m >= T {
					break
				}
			}
		}()
	}

	wg.Add(N)

	for i := range N {
		go func() {
			defer wg.Done()

			for {
				msg, st, end := q.Consume(true)
				if msg == bufq.Closed {
					break
				}
				if msg < 0 {
					tb.Errorf("msg: %x", msg)
					break
				}

				tb.Logf("job s_%x, msg %03x, bs %3x-%3x: %s  written by %4x", i, msg, st, end, b[st:end], meta[q.Msg(msg)])

				x, err := strconv.ParseUint(string(b[st:st+4]), 16, 32)
				if err != nil {
					tb.Errorf("parse x: %v", err)
				}

				read[x]++

				runtime.Gosched()

				q.Done(msg)

				runtime.Gosched()
			}
		}()
	}

	wg.Add(N)

	for i := range N {
		go func() {
			defer wg.Done()

			ms := make([]bufq.Message, 3)

			for {
				m := q.ConsumeN(true, ms)
				if m == bufq.Closed {
					break
				}
				if m < 0 {
					tb.Errorf("msg: %x", m)
					break
				}

				for _, mm := range ms[:m] {
					msg := mm.Msg
					st, end := mm.StartEnd()

					tb.Logf("job %d_%x, msg %03x, bs %3x-%3x: %s  written by %4x", m, i, msg, st, end, b[st:end], meta[q.Msg(msg)])

					x, err := strconv.ParseUint(string(b[st:st+4]), 16, 32)
					if err != nil {
						tb.Errorf("parse x: %v", err)
					}

					read[x]++
				}

				runtime.Gosched()

				q.DoneN(ms[:m])

				runtime.Gosched()
			}
		}()
	}

	wwg.Wait()
	q.Close()

	wg.Wait()

	for i := range T {
		if read[i] != 1 {
			tb.Errorf("x %x = %x", i, read[i])
		}
	}
}
