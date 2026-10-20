package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

var ErrLimit = errors.New("catalog portability limit exceeded")

const (
	MaxStreamBytes          int64 = 64 << 30
	MaxStreamEvents         int64 = 1_000_000
	MaxStreamProjectionRows int64 = 10_000_000
	streamMagic                   = "juicemount.catalog.export\x00\x01"
	maxEventPayload               = 8 + MaxRequestBytes + 1024
)

type Limits struct {
	MaxBytes          int64
	MaxEvents         int64
	MaxProjectionRows int64
}

func (l Limits) validate() error {
	if l.MaxBytes <= 0 || l.MaxBytes > MaxStreamBytes || l.MaxEvents <= 0 || l.MaxEvents > MaxStreamEvents || l.MaxProjectionRows <= 0 || l.MaxProjectionRows > MaxStreamProjectionRows {
		return invalid("portability budgets")
	}
	return nil
}

type streamHeader struct {
	Version       int                  `json:"version"`
	SchemaVersion int                  `json:"schema_version"`
	LibraryID     continuity.LibraryID `json:"library_id"`
	EventCount    int64                `json:"event_count"`
}

type streamFooter struct {
	EventCount       int64  `json:"event_count"`
	ProjectionRows   int64  `json:"projection_rows"`
	ProjectionSHA256 string `json:"projection_sha256"`
	StreamSHA256     string `json:"stream_sha256"`
}

func canonicalJSON(data []byte, value any) error {
	if err := strictJSON(data, value); err != nil {
		return formatError(err)
	}
	encoded, err := json.Marshal(value)
	if err != nil || !bytes.Equal(encoded, data) {
		return corrupt("noncanonical portability JSON")
	}
	return nil
}

func frameHeader(kind byte, size int) [5]byte {
	var header [5]byte
	header[0] = kind
	binary.BigEndian.PutUint32(header[1:], uint32(size))
	return header
}

func streamHash() hash.Hash {
	h := sha256.New()
	_, _ = h.Write([]byte("juicemount.catalog.export.stream\x00\x01"))
	return h
}

func finishStreamHash(h hash.Hash, events, rows int64, projection string) (string, error) {
	if err := (continuity.Content{SHA256: projection}).Validate(); err != nil {
		return "", corrupt("projection digest spelling")
	}
	var counts [16]byte
	binary.BigEndian.PutUint64(counts[:8], uint64(events))
	binary.BigEndian.PutUint64(counts[8:], uint64(rows))
	_, _ = h.Write(counts[:])
	raw, _ := hex.DecodeString(projection)
	_, _ = h.Write(raw)
	return hex.EncodeToString(h.Sum(nil)), nil
}

type streamInput struct {
	ctx       context.Context
	r         io.Reader
	max, used int64
	eof       bool
	empty     int
}

// exact does not discard an error accompanying a complete read, unlike ReadFull.
func (s *streamInput) exact(p []byte) error {
	if int64(len(p)) > s.max-s.used {
		return ErrLimit
	}
	for len(p) > 0 {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		if s.eof {
			return io.ErrUnexpectedEOF
		}
		n, err := s.r.Read(p)
		if n < 0 || n > len(p) {
			return corrupt("impossible reader count")
		}
		s.used += int64(n)
		p = p[n:]
		if n > 0 {
			s.empty = 0
		} else if err == nil {
			s.empty++
			if s.empty >= 100 {
				return io.ErrNoProgress
			}
		}
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			s.eof = true
			if len(p) > 0 {
				return io.ErrUnexpectedEOF
			}
		}
	}
	return nil
}

func (s *streamInput) frame(expected byte, h hash.Hash) ([]byte, error) {
	var header [5]byte
	if err := s.exact(header[:]); err != nil {
		return nil, err
	}
	if header[0] != expected {
		return nil, corrupt("unexpected stream frame kind")
	}
	size := binary.BigEndian.Uint32(header[1:])
	max := uint32(1024)
	if expected == 2 || expected == 4 {
		max = maxEventPayload
	}
	if size == 0 || size > max {
		return nil, corrupt("stream frame length")
	}
	if int64(size) > s.max-s.used {
		return nil, ErrLimit
	}
	payload := make([]byte, int(size))
	if err := s.exact(payload); err != nil {
		return nil, err
	}
	if h != nil {
		_, _ = h.Write(header[:])
		_, _ = h.Write(payload)
	}
	return payload, nil
}

func (s *streamInput) end() error {
	for {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		if s.eof {
			return nil
		}
		// One bounded probe is permitted even at the exact stream byte budget.
		var extra [1]byte
		n, err := s.r.Read(extra[:])
		if n < 0 || n > 1 {
			return corrupt("impossible reader count")
		}
		if err != nil && err != io.EOF {
			return err
		}
		if n > 0 {
			return corrupt("trailing stream bytes")
		}
		if err == io.EOF {
			return nil
		}
		s.empty++
		if s.empty >= 100 {
			return io.ErrNoProgress
		}
	}
}

type streamOutput struct {
	ctx       context.Context
	w         io.Writer
	max, used int64
}

func (s *streamOutput) write(p []byte, h hash.Hash) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if int64(len(p)) > s.max-s.used {
		return ErrLimit
	}
	n, err := s.w.Write(p)
	if n < 0 || n > len(p) {
		return corrupt("impossible writer count")
	}
	s.used += int64(n)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrShortWrite
	}
	if h != nil {
		_, _ = h.Write(p)
	}
	return nil
}

func (s *streamOutput) frame(kind byte, payload []byte, h hash.Hash) error {
	if int64(len(payload))+5 > s.max-s.used {
		return ErrLimit
	}
	header := frameHeader(kind, len(payload))
	if err := s.write(header[:], h); err != nil {
		return err
	}
	return s.write(payload, h)
}

func encodeEvent(e eventRow) []byte {
	payload := make([]byte, 0, 8+len(e.request)+len(e.receipt))
	payload = binary.BigEndian.AppendUint32(payload, uint32(len(e.request)))
	payload = append(payload, e.request...)
	payload = binary.BigEndian.AppendUint32(payload, uint32(len(e.receipt)))
	return append(payload, e.receipt...)
}

func decodeStreamEvent(payload []byte, library continuity.LibraryID, sequence int64) (eventRow, Operation, error) {
	if len(payload) < 8 {
		return eventRow{}, Operation{}, corrupt("short event frame")
	}
	n := binary.BigEndian.Uint32(payload[:4])
	if n == 0 || n > MaxRequestBytes || int64(n) > int64(len(payload))-8 {
		return eventRow{}, Operation{}, corrupt("event request length")
	}
	requestEnd := 4 + int(n)
	m := binary.BigEndian.Uint32(payload[requestEnd : requestEnd+4])
	if m == 0 || m > 1024 || int64(requestEnd)+4+int64(m) != int64(len(payload)) {
		return eventRow{}, Operation{}, corrupt("event receipt length")
	}
	var receipt Receipt
	data := payload[requestEnd+4:]
	if err := canonicalJSON(data, &receipt); err != nil {
		return eventRow{}, Operation{}, err
	}
	if receipt.Sequence != sequence {
		return eventRow{}, Operation{}, corrupt("event sequence discontinuity")
	}
	e := eventRow{sequence: sequence, library: library, id: receipt.OperationID, request: payload[4:requestEnd], digest: receipt.PayloadSHA256, timestamp: receipt.RecordedUnixNano, receipt: data}
	event, err := e.decode(library)
	if err != nil {
		return eventRow{}, Operation{}, err
	}
	return e, event.Operation, nil
}

func protocolError(stage string, err error) error { return fmt.Errorf("catalog %s: %w", stage, err) }
