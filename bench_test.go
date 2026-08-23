package bufq_test

import (
	"fmt"
	"testing"

	"nikand.dev/go/bufq"
)

func BenchmarkQueue(tb *testing.B) {
	for _, n := range []int{0x40, 0x100, 0x400} {
		tb.Run(fmt.Sprintf("n_%03x/steady", n), func(tb *testing.B) { benchSteady(tb, n) })
		tb.Run(fmt.Sprintf("n_%03x/slow_consumer", n), func(tb *testing.B) { benchParked(tb, n, false) })
		tb.Run(fmt.Sprintf("n_%03x/slow_producer", n), func(tb *testing.B) { benchParked(tb, n, true) })
	}
}

func benchSteady(tb *testing.B, n int) {
	q := bufq.New(n, 0x10*n)

	tb.ReportAllocs()
	tb.ResetTimer()

	for range tb.N {
		msg, _, _ := q.Allocate(0x10, 0, false)
		if msg < 0 {
			tb.Fatalf("allocate: %v", bufq.Error(msg))
		}

		q.Commit(msg, 0x10)

		msg, _, _ = q.Consume(false)
		if msg < 0 {
			tb.Fatalf("consume: %v", bufq.Error(msg))
		}

		q.Done(msg)
	}
}

// benchParked holds the head message while the rest of the queue churns,
// so every Consume has to look past the slots behind it.
func benchParked(tb *testing.B, n int, producer bool) {
	q := bufq.New(n, 0x10*n)

	tb.ReportAllocs()
	tb.ResetTimer()

	for i := 0; i < tb.N; {
		head, _, _ := q.Allocate(0x10, 0, false)
		if head < 0 {
			tb.Fatalf("allocate head: %v", bufq.Error(head))
		}

		if !producer {
			q.Commit(head, 0x10)

			head, _, _ = q.Consume(false)
			if head < 0 {
				tb.Fatalf("consume head: %v", bufq.Error(head))
			}
		}

		for j := 0; j < n-2 && i < tb.N; j, i = j+1, i+1 {
			msg, _, _ := q.Allocate(0x10, 0, false)
			if msg < 0 {
				tb.Fatalf("allocate: %v", bufq.Error(msg))
			}

			q.Commit(msg, 0x10)

			msg, _, _ = q.Consume(false)
			if msg < 0 {
				tb.Fatalf("consume: %v", bufq.Error(msg))
			}

			q.Done(msg)
		}

		if producer {
			q.Commit(head, 0x10)

			head, _, _ = q.Consume(false)
			if head < 0 {
				tb.Fatalf("consume head: %v", bufq.Error(head))
			}
		}

		q.Done(head)
	}
}
