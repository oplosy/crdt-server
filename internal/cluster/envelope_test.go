package cluster

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/mesutokul/ycollab/internal/crdt/lib0"
)

func TestEnvelopeRoundTrips(t *testing.T) {
	for _, kind := range []Kind{KindUpdate, KindAwareness, KindStateVector} {
		// The largest origin an id can be: lib0's varUint mirrors JavaScript and
		// stops at 2^53-1, which is why NewNodeID masks to 53 bits.
		env := Envelope{Origin: 1<<53 - 1, Kind: kind, Payload: []byte{1, 2, 3, 0, 255}}
		raw, err := env.Encode()
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		got, err := Decode(raw)
		if err != nil {
			t.Fatalf("%v: decode: %v", kind, err)
		}
		if got.Origin != env.Origin || got.Kind != env.Kind || !bytes.Equal(got.Payload, env.Payload) {
			t.Fatalf("%v: round trip gave %+v, want %+v", kind, got, env)
		}
	}
}

// An empty payload is a real case: a state vector for a document nobody has
// written to encodes to a single zero byte, and a lost length prefix would turn
// that into a decode error at exactly the moment anti-entropy matters most.
func TestEnvelopeCarriesAnEmptyPayload(t *testing.T) {
	raw, err := Envelope{Origin: 1, Kind: KindStateVector}.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Payload) != 0 {
		t.Fatalf("payload is %x, want empty", got.Payload)
	}
}

func TestEnvelopePreservesFullIncarnationRange(t *testing.T) {
	for _, incarnation := range []int64{0, 1<<53 + 17, -1 << 63, 1<<63 - 1} {
		want := Envelope{Origin: 1, Kind: KindUpdate, HasIncarnation: true, Incarnation: incarnation}
		raw, err := want.Encode()
		if err != nil {
			t.Fatalf("encode %d: %v", incarnation, err)
		}
		got, err := Decode(raw)
		if err != nil || !got.HasIncarnation || got.Incarnation != incarnation {
			t.Fatalf("round trip %d = %+v, %v", incarnation, got, err)
		}
	}
	legacy, err := (Envelope{Origin: 1, Kind: KindUpdate, HasIncarnation: true}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	noStore, err := Decode(legacy)
	if err != nil || !noStore.HasIncarnation || noStore.Incarnation != 0 {
		t.Fatalf("legacy zero token = %+v, %v", noStore, err)
	}
	noDB, err := (Envelope{Origin: 1, Kind: KindUpdate}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	noStore, err = Decode(noDB)
	if err != nil || noStore.HasIncarnation {
		t.Fatalf("no-store marker = %+v, %v", noStore, err)
	}
}

// A node id has to be encodable, so the generator's masking is part of the
// contract rather than an implementation detail.
func TestNewNodeIDIsEncodable(t *testing.T) {
	for range 1000 {
		id := NewNodeID()
		if id == 0 {
			t.Fatal("generated the reserved zero id")
		}
		raw, err := Envelope{Origin: id, Kind: KindUpdate}.Encode()
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		got, err := Decode(raw)
		if err != nil {
			t.Fatalf("node id %d will not encode: %v", id, err)
		}
		if got.Origin != id {
			t.Fatalf("node id %d came back as %d", id, got.Origin)
		}
	}
}

func TestEnvelopeRejectsWhatItCannotTrust(t *testing.T) {
	valid, err := Envelope{Origin: 7, Kind: KindUpdate, Payload: []byte{9}}.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	cases := []struct {
		name string
		buf  []byte
		want error
	}{
		{"legacy version", []byte{0, 7, 0, 0, 1, 9}, ErrVersion},
		{"future version", []byte{2, 7, 0, 0, 1, 9}, ErrVersion},
		{"unknown kind", []byte{1, 7, 99, 0, 1, 9}, ErrKind},
		{"trailing bytes", append(append([]byte(nil), valid...), 0xff), ErrTrailingBytes},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Decode(tc.buf); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}

	// A truncated envelope must be an error rather than a panic: it is what a
	// half-written message or a mismatched build looks like.
	for i := range len(valid) {
		if _, err := Decode(valid[:i]); err == nil {
			t.Fatalf("accepted a %d-byte prefix of a %d-byte envelope", i, len(valid))
		}
	}
}

func FuzzDecodeNeverPanics(f *testing.F) {
	seed, err := Envelope{Origin: 1, Kind: KindUpdate, Payload: []byte{1, 2}}.Encode()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte{0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, buf []byte) {
		env, err := Decode(buf)
		if err != nil {
			return
		}
		// Anything that decodes must survive being written back out and read
		// again, because that is what a replica does with it.
		raw, err := env.Encode()
		if err != nil {
			t.Fatalf("a decoded envelope will not re-encode: %v", err)
		}
		again, err := Decode(raw)
		if err != nil {
			t.Fatalf("re-encoded envelope will not decode: %v", err)
		}
		if again.Origin != env.Origin || again.Kind != env.Kind || again.HasIncarnation != env.HasIncarnation ||
			again.Incarnation != env.Incarnation || !bytes.Equal(again.Payload, env.Payload) {
			t.Fatalf("round trip changed %+v into %+v", env, again)
		}
	})
}

// The in-process bus is what the room's fanout tests run on, so its delivery
// rules have to be the ones Redis has: everybody subscribed to the channel gets
// the message, the publisher included, and nobody subscribed to another channel
// does.
func TestMemoryBusDeliversToEverySubscriberIncludingThePublisher(t *testing.T) {
	bus := NewMemory()
	ctx := context.Background()

	var a, b, other []Envelope
	subA, err := bus.Subscribe(ctx, "doc", func(e Envelope) { a = append(a, e) })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bus.Subscribe(ctx, "doc", func(e Envelope) { b = append(b, e) }); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.Subscribe(ctx, "elsewhere", func(e Envelope) { other = append(other, e) }); err != nil {
		t.Fatal(err)
	}

	if err := bus.Publish(ctx, "doc", Envelope{Origin: 1, Kind: KindUpdate, Payload: []byte{42}}); err != nil {
		t.Fatal(err)
	}
	if len(a) != 1 || len(b) != 1 {
		t.Fatalf("delivered %d and %d, want one each", len(a), len(b))
	}
	if len(other) != 0 {
		t.Fatalf("a subscriber on another channel got %d messages", len(other))
	}
	if !bytes.Equal(a[0].Payload, []byte{42}) {
		t.Fatalf("payload came through as %x", a[0].Payload)
	}

	if err := subA.Close(); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(ctx, "doc", Envelope{Origin: 1, Kind: KindUpdate}); err != nil {
		t.Fatal(err)
	}
	if len(a) != 1 {
		t.Fatalf("a closed subscription still received %d messages", len(a))
	}
	if len(b) != 2 {
		t.Fatalf("the remaining subscriber got %d messages, want 2", len(b))
	}
}

// lib0's decoder is more permissive than its encoder: it will read an integer
// above 2^53-1, which the encoder refuses to write. An envelope carrying one
// used to decode and then encode to a one-byte buffer, silently - every replica
// would log "undecodable" with nothing saying why.
//
// The seed is the one the fuzzer found.
func TestAnOriginTooLargeToEncodeIsRefused(t *testing.T) {
	// version 1, then a varUint of seven continuation bytes that reads as
	// 27,362,913,877,714,637 - three times what lib0 will write - then kind 2
	// and an empty payload.
	buf := []byte{0x01, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0x30, 0x02, 0x00, 0x00}
	if _, err := Decode(buf); !errors.Is(err, ErrOriginOutOfRange) {
		t.Fatalf("Decode returned %v, want ErrOriginOutOfRange", err)
	}
	// And building one by hand is an error rather than a short buffer.
	if _, err := (Envelope{Origin: lib0.MaxSafeInteger + 1, Kind: KindUpdate}).Encode(); err == nil {
		t.Error("Encode accepted an origin it cannot write")
	}
	// The largest legal origin still round-trips, so the check is not off by one.
	raw, err := (Envelope{Origin: lib0.MaxSafeInteger, Kind: KindUpdate, Payload: []byte{1}}).Encode()
	if err != nil {
		t.Fatalf("the largest legal origin: %v", err)
	}
	got, err := Decode(raw)
	if err != nil || got.Origin != lib0.MaxSafeInteger {
		t.Errorf("MaxSafeInteger round-tripped to %d (err %v)", got.Origin, err)
	}
}
