// Package journal is an append-only log of opaque records. Append returns
// only after the record is fsynced, so an acknowledged record survives
// kill -9 and power loss. Open recovers from a torn tail and refuses to open
// a file that is damaged anywhere else.
//
// On-disk format:
//
//	file  : "WJNL0001" frame*
//	frame : "WJR1" | len u32 LE | crc32c(len || payload) u32 LE | payload
//
// len is between 1 and MaxRecord.
package journal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

const (
	fileHeader      = "WJNL0001"
	frameMagic      = "WJR1"
	frameHeaderSize = 12
	// MaxRecord is the largest payload Append accepts.
	MaxRecord = 64 << 20
)

var (
	ErrLocked     = errors.New("journal: locked by another writer")
	ErrCorrupt    = errors.New("journal: corrupt")
	ErrBadHeader  = errors.New("journal: bad file header")
	ErrPoisoned   = errors.New("journal: poisoned by an earlier write or fsync failure")
	ErrClosed     = errors.New("journal: closed")
	ErrRecordSize = errors.New("journal: record size out of range")
)

// CorruptError reports an invalid frame with a valid frame somewhere after
// it. A torn write only damages the tail, so this is real corruption, and
// truncating would drop acknowledged records.
type CorruptError struct{ Offset int64 }

func (e *CorruptError) Error() string {
	return fmt.Sprintf("journal: corrupt frame at offset %d with valid frames after it", e.Offset)
}

func (e *CorruptError) Unwrap() error { return ErrCorrupt }

// File is the storage seam. *os.File satisfies it through Open; CrashFile
// satisfies it in tests that model power loss.
type File interface {
	ReadAt(p []byte, off int64) (int, error)
	WriteAt(p []byte, off int64) (int, error)
	Truncate(size int64) error
	Sync() error
	Size() (int64, error)
}

// Report says what Open found.
type Report struct {
	Records        int
	Created        bool
	TruncatedBytes int64
}

// Log is safe for concurrent use. Appends are serialized, and each one
// fsyncs before it returns.
type Log struct {
	mu      sync.Mutex
	f       File
	release func() error
	tail    int64
	err     error
	closed  bool
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Open opens or creates the journal at path and takes an exclusive flock on
// it. The kernel drops the lock when the process dies, including on SIGKILL.
// visit is called for every record in file order before Open returns; it may
// keep payload. If visit returns an error, Open returns it and leaves the
// file unchanged.
func Open(path string, visit func(off int64, payload []byte) error) (*Log, Report, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, Report{}, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, Report{}, fmt.Errorf("%w: %s", ErrLocked, path)
		}
		return nil, Report{}, fmt.Errorf("journal: flock %s: %w", path, err)
	}
	l, rep, err := open(osFile{f}, visit, func() error { return syncDir(filepath.Dir(path)) })
	if err != nil {
		f.Close()
		return nil, Report{}, err
	}
	l.release = f.Close
	return l, rep, nil
}

// OpenFile is Open over an arbitrary File, without locking.
func OpenFile(f File, visit func(off int64, payload []byte) error) (*Log, Report, error) {
	return open(f, visit, func() error { return nil })
}

func open(f File, visit func(int64, []byte) error, syncParent func() error) (*Log, Report, error) {
	size, err := f.Size()
	if err != nil {
		return nil, Report{}, err
	}
	buf := make([]byte, size)
	if size > 0 {
		if _, err := f.ReadAt(buf, 0); err != nil && err != io.EOF {
			return nil, Report{}, err
		}
	}

	if size < int64(len(fileHeader)) {
		// Empty, or a crash while the header was being written.
		if !strings.HasPrefix(fileHeader, string(buf)) {
			return nil, Report{}, ErrBadHeader
		}
		if _, err := f.WriteAt([]byte(fileHeader), 0); err != nil {
			return nil, Report{}, err
		}
		if err := f.Sync(); err != nil {
			return nil, Report{}, err
		}
		if err := syncParent(); err != nil {
			return nil, Report{}, err
		}
		return &Log{f: f, tail: int64(len(fileHeader))}, Report{Created: true}, nil
	}
	if string(buf[:len(fileHeader)]) != fileHeader {
		return nil, Report{}, ErrBadHeader
	}

	type frame struct {
		off     int64
		payload []byte
	}
	var frames []frame
	end := int64(len(fileHeader))
	for {
		payload, ok := frameAt(buf, end)
		if !ok {
			break
		}
		frames = append(frames, frame{end, payload})
		end += frameHeaderSize + int64(len(payload))
	}
	if end < size && validFrameAfter(buf, end+1) {
		return nil, Report{}, &CorruptError{Offset: end}
	}

	if visit != nil {
		for _, fr := range frames {
			if err := visit(fr.off, fr.payload); err != nil {
				return nil, Report{}, err
			}
		}
	}

	rep := Report{Records: len(frames)}
	if end < size {
		if err := f.Truncate(end); err != nil {
			return nil, Report{}, err
		}
		if err := f.Sync(); err != nil {
			return nil, Report{}, err
		}
		rep.TruncatedBytes = size - end
	}
	return &Log{f: f, tail: end}, rep, nil
}

func frameAt(buf []byte, off int64) ([]byte, bool) {
	if int64(len(buf))-off < frameHeaderSize || string(buf[off:off+4]) != frameMagic {
		return nil, false
	}
	n := int64(binary.LittleEndian.Uint32(buf[off+4:]))
	if n == 0 || n > MaxRecord || off+frameHeaderSize+n > int64(len(buf)) {
		return nil, false
	}
	payload := buf[off+frameHeaderSize : off+frameHeaderSize+n]
	if checksum(buf[off+4:off+8], payload) != binary.LittleEndian.Uint32(buf[off+8:]) {
		return nil, false
	}
	return payload, true
}

func validFrameAfter(buf []byte, from int64) bool {
	for from < int64(len(buf)) {
		i := bytes.Index(buf[from:], []byte(frameMagic))
		if i < 0 {
			return false
		}
		if _, ok := frameAt(buf, from+int64(i)); ok {
			return true
		}
		from += int64(i) + 1
	}
	return false
}

func checksum(lenBytes, payload []byte) uint32 {
	return crc32.Update(crc32.Checksum(lenBytes, castagnoli), castagnoli, payload)
}

// Append writes one record and fsyncs it. A failed write or fsync leaves the
// file in an unknown state, so it poisons the Log: every later Append fails
// with ErrPoisoned, and the next Open recovers the file.
func (l *Log) Append(payload []byte) error {
	if len(payload) == 0 || len(payload) > MaxRecord {
		return fmt.Errorf("%w: %d bytes", ErrRecordSize, len(payload))
	}
	frame := make([]byte, frameHeaderSize+len(payload))
	copy(frame, frameMagic)
	binary.LittleEndian.PutUint32(frame[4:], uint32(len(payload)))
	copy(frame[frameHeaderSize:], payload)
	binary.LittleEndian.PutUint32(frame[8:], checksum(frame[4:8], payload))

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if l.err != nil {
		return l.err
	}
	if _, err := l.f.WriteAt(frame, l.tail); err != nil {
		l.err = fmt.Errorf("%w: write: %v", ErrPoisoned, err)
		return fmt.Errorf("journal: write: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		l.err = fmt.Errorf("%w: fsync: %v", ErrPoisoned, err)
		return fmt.Errorf("journal: fsync: %w", err)
	}
	l.tail += int64(len(frame))
	return nil
}

// Close releases the file and its lock. Every acknowledged record is
// already durable, so Close does not sync.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	if l.release != nil {
		return l.release()
	}
	return nil
}

type osFile struct{ *os.File }

func (f osFile) Size() (int64, error) {
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
