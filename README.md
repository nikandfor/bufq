[![Documentation](https://pkg.go.dev/badge/nikand.dev/go/bufq)](https://pkg.go.dev/nikand.dev/go/bufq?tab=doc)
[![Go workflow](https://github.com/nikandfor/bufq/actions/workflows/go.yml/badge.svg)](https://github.com/nikandfor/bufq/actions/workflows/go.yml)
[![CircleCI](https://circleci.com/gh/nikandfor/bufq.svg?style=svg)](https://circleci.com/gh/nikandfor/bufq)
[![codecov](https://codecov.io/gh/nikandfor/bufq/tags/latest/graph/badge.svg)](https://codecov.io/gh/nikandfor/bufq)
[![Go Report Card](https://goreportcard.com/badge/nikand.dev/go/bufq)](https://goreportcard.com/report/nikand.dev/go/bufq)
![GitHub tag (latest SemVer)](https://img.shields.io/github/v/tag/nikandfor/bufq?sort=semver)

# bufq

`bufq` is a queue for efficiently passing chunks of a ring buffer along with their metadata.
The initial task was to read and process over 1 Gbit/s of small UDP packets.

## Workflow

A message travels through two hands.

	producer:  Allocate -> write b[st:end] and meta[msg] -> Commit
	consumer:  Consume  -> read  b[st:end] and meta[msg] -> Done

`Allocate` reserves a slot and a chunk of the ring buffer, `Commit` publishes the bytes actually written,
or cancels the message. `Consume` gives it to exactly one consumer, `Done` releases it.

Committed messages are consumed in any order, but buffer space comes back in allocation order:
a message in flight holds its own chunk and everything allocated after it.
So the buffer stays a plain ring and never fragments.

Every step has a batch form (`AllocateN`, `CommitN`, `ConsumeN`, `DoneN`) and can block or not.
`Close` wakes all the waiters up; messages already committed are still consumed.

## Usage

The queue operates solely with indexes, which makes it independent of the buffer and metadata types, as well as their storage locations.
The buffer itself can be a slice, a memory-mapped file, or any other type.

A common pattern is that there are one or more producers and one or more consumers.
Each producer and consumer can produce or consume a single message or multiple messages at a time.

A basic example: a single UDP reader reads packets into a shared buffer as fast as it can, while a few workers process those packets.

```go
	type Meta struct {
		Addr netip.AddrPort
	}

	const MaxPacketSize, Workers = 0x100, 4

	meta := make([]Meta, 0x1000)               // meta info buffer
	b := make([]byte, len(meta)*MaxPacketSize) // data buffer

	q := bufq.New(len(meta), len(b))

	var p *net.UDPConn // = ...

	go func() (err error) {
		defer q.Close()

		for {
			msg, st, end := q.Allocate(MaxPacketSize, 16, true)
			if msg < 0 {
				return bufq.Error(msg)
			}

			// meta[msg] and b[st:end] can be safely used between Allocate and Commit calls.

			n, addr, err := p.ReadFromUDPAddrPort(b[st:end])
			if err != nil {
				q.Commit(msg, bufq.Cancel) // unlock message buffer
				return err
			}

			meta[msg].Addr = addr

			q.Commit(msg, n)
		}
	}()

	for worker := 0; worker < Workers; worker++ {
		go func() (err error) {
			for {
				msg, st, end := q.Consume(true)
				if msg < 0 {
					return bufq.Error(msg)
				}

				// meta[msg] and b[st:end] can be safely used between Consume and Done calls.

				fmt.Printf("worker %d: message from %v: %s\n", worker, meta[msg].Addr, b[st:end])

				q.Done(msg)
			}
		}()
	}
```

The reader might read a batch of packets at a time using [`ReadBatch`](https://pkg.go.dev/golang.org/x/net/ipv6#PacketConn.ReadBatch).
In this case, `AllocateN` is used. Refer to the [example_n_test.go](example_n_test.go) for clarification.
