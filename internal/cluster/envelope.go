// Package cluster carries one document's traffic between server replicas.
//
// The unit is an Envelope: who sent it, what kind of thing it is, and the
// payload, which is always bytes the rest of the system already understands - a
// Yjs update, an awareness update, or an encoded state vector. Nothing in here
// interprets a payload.
//
// The outer fields use lib0, as the client protocol does. The internal v1
// incarnation token is fixed-width big-endian because PostgreSQL BIGINT uses
// the full signed 64-bit range, beyond lib0's safe integer limit.
package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/mesutokul/ycollab/internal/crdt/lib0"
)

// Kind says what an envelope's payload is.
type Kind uint64

const (
	// KindUpdate carries a Yjs update: either one a client produced, or a diff
	// computed in answer to a KindStateVector.
	KindUpdate Kind = 0
	// KindAwareness carries a y-protocols awareness update payload.
	KindAwareness Kind = 1
	// KindStateVector carries an encoded state vector, published periodically so
	// a replica that missed a message finds out. Redis Pub/Sub is at-most-once,
	// so this is the only thing standing between a dropped message and a
	// permanent divergence; see DECISIONS C1.
	KindStateVector Kind = 2
)

// envelopeVersion prefixes every envelope. Two replicas running different
// builds is normal during a rolling restart, and a version byte turns "the
// bytes stopped making sense" into a message we can log and skip.
const envelopeVersion uint64 = 1

var (
	// ErrVersion means the envelope was written by a build that does not agree
	// with this one about the format.
	ErrVersion = errors.New("cluster: unknown envelope version")
	// ErrKind means the envelope kind is not one this build knows.
	ErrKind = errors.New("cluster: unknown envelope kind")
	// ErrTrailingBytes means an envelope carried bytes after a complete message,
	// the same stance internal/protocol takes on client frames.
	ErrTrailingBytes = errors.New("cluster: trailing bytes after envelope")
	// ErrOriginOutOfRange means the origin is larger than lib0 can encode. The
	// format cannot carry it, so accepting it would mean holding a value that
	// cannot be written back out - see Decode.
	ErrOriginOutOfRange = errors.New("cluster: origin out of range")
)

// An Envelope is one message between replicas.
type Envelope struct {
	// Origin identifies the replica that published this. A subscriber sees its
	// own messages - Redis Pub/Sub delivers to every subscriber of a channel,
	// publisher included - and drops them by comparing this against its own node
	// id. That comparison is the whole loop prevention.
	Origin uint64
	Kind   Kind
	// HasIncarnation distinguishes a persisted legacy incarnation zero from
	// messages published by a server without persistence.
	HasIncarnation bool
	Incarnation    int64
	// Payload aliases the buffer Decode was given.
	Payload []byte
}

// Encode returns the wire form of an envelope.
//
// It returns an error rather than bytes-and-hope. lib0 refuses an integer above
// 2^53-1, and the encoder records that instead of raising it - so an origin out
// of range used to come back as a one-byte buffer that every replica logged as
// undecodable, with nothing anywhere saying why. Found by the fuzzer: it
// produced an envelope that decoded and then would not survive being written
// back out.
func (e Envelope) Encode() ([]byte, error) {
	enc := lib0.NewEncoderSize(len(e.Payload) + 25)
	enc.WriteVarUint(envelopeVersion)
	enc.WriteVarUint(e.Origin)
	enc.WriteVarUint(uint64(e.Kind))
	if e.HasIncarnation {
		enc.WriteUint8(1)
		var identity [8]byte
		binary.BigEndian.PutUint64(identity[:], uint64(e.Incarnation))
		for _, b := range identity {
			enc.WriteUint8(b)
		}
	} else {
		enc.WriteUint8(0)
	}
	enc.WriteVarUint8Array(e.Payload)
	if err := enc.Err(); err != nil {
		return nil, fmt.Errorf("cluster: encode envelope: %w", err)
	}
	return enc.Bytes(), nil
}

// Decode parses one envelope. The payload aliases buf, so the caller must
// not reuse buf until the payload is no longer needed. Trailing bytes and
// unknown versions/kinds are refused rather than skipped: a rolling restart
// with mixed builds should be visible in the logs, not silent.
func Decode(buf []byte) (Envelope, error) {
	d := lib0.NewDecoder(buf)
	version, err := d.ReadVarUint()
	if err != nil {
		return Envelope{}, err
	}
	if version != envelopeVersion {
		return Envelope{}, fmt.Errorf("%w: %d", ErrVersion, version)
	}
	origin, err := d.ReadVarUint()
	if err != nil {
		return Envelope{}, err
	}
	if origin > lib0.MaxSafeInteger {
		// lib0's decoder is more permissive than its encoder. Accepting a value
		// here that Encode cannot write would leave the rest of the system
		// holding an envelope it can never relay, so it is refused at the door.
		// NewNodeID masks its ids for the same reason.
		return Envelope{}, fmt.Errorf("%w: %d", ErrOriginOutOfRange, origin)
	}
	kind, err := d.ReadVarUint()
	if err != nil {
		return Envelope{}, err
	}
	switch Kind(kind) {
	case KindUpdate, KindAwareness, KindStateVector:
	default:
		return Envelope{}, fmt.Errorf("%w: %d", ErrKind, kind)
	}
	hasIncarnation, err := d.ReadUint8()
	if err != nil {
		return Envelope{}, err
	}
	if hasIncarnation > 1 {
		return Envelope{}, fmt.Errorf("cluster: invalid incarnation marker %d", hasIncarnation)
	}
	var incarnation int64
	if hasIncarnation == 1 {
		var identity [8]byte
		for i := range identity {
			identity[i], err = d.ReadUint8()
			if err != nil {
				return Envelope{}, err
			}
		}
		incarnation = int64(binary.BigEndian.Uint64(identity[:]))
	}
	payload, err := d.ReadVarUint8Array()
	if err != nil {
		return Envelope{}, err
	}
	if !d.Done() {
		return Envelope{}, ErrTrailingBytes
	}
	return Envelope{Origin: origin, Kind: Kind(kind), HasIncarnation: hasIncarnation == 1, Incarnation: incarnation, Payload: payload}, nil
}

func (k Kind) String() string {
	switch k {
	case KindUpdate:
		return "update"
	case KindAwareness:
		return "awareness"
	case KindStateVector:
		return "state_vector"
	default:
		return fmt.Sprintf("kind(%d)", uint64(k))
	}
}
