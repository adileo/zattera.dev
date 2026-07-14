package logstore

import (
	"bufio"
	"encoding/binary"
	"io"
	"time"
)

// On-disk record: uvarint(bodyLen) | body, where body =
//   int64 time-unixnano (little-endian) | 1 byte flags | line bytes.
// flags bit0 = stderr. Records are appended in write order; a torn tail (a
// crash mid-write, since we do not fsync) is detected on read and ignored.

const recordHeaderBytes = 8 + 1 // time + flags

// appendRecord encodes e onto buf and returns the extended slice.
func appendRecord(buf []byte, e Entry) []byte {
	body := recordHeaderBytes + len(e.Line)
	buf = binary.AppendUvarint(buf, uint64(body))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(e.Time.UnixNano()))
	var flags byte
	if e.Stderr {
		flags = 1
	}
	buf = append(buf, flags)
	buf = append(buf, e.Line...)
	return buf
}

// scanRecords reads length-prefixed records from r, invoking fn for each. It
// stops cleanly at EOF or a truncated tail (best-effort logs; a crash mid-write
// leaves a torn final record). fn returning false stops iteration early.
func scanRecords(r *bufio.Reader, fn func(Entry) bool) {
	for {
		bodyLen, err := binary.ReadUvarint(r)
		if err != nil || bodyLen < recordHeaderBytes {
			return // EOF, torn varint, or corrupt length
		}
		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(r, body); err != nil {
			return // truncated final record
		}
		nano := int64(binary.LittleEndian.Uint64(body[:8]))
		e := Entry{
			Time:   time.Unix(0, nano),
			Stderr: body[8]&1 != 0,
			Line:   string(body[9:]),
		}
		if !fn(e) {
			return
		}
	}
}
