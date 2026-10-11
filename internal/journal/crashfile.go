package journal

import (
	"io"
	"sync"
)

// CrashFile is an in-memory File that models power loss rather than kill -9.
// Writes and truncates change only the visible image. Sync makes the visible
// image durable. LosePower reverts to the durable image plus a torn prefix of
// whatever was written past its end.
type CrashFile struct {
	// FailSync and FailWrite, when set, make Sync and WriteAt fail without
	// effect.
	FailSync  error
	FailWrite error

	mu      sync.Mutex
	data    []byte
	durable []byte
}

func NewCrashFile() *CrashFile { return &CrashFile{} }

// LosePower keeps the durable image and the first tear bytes written past
// its end. With zeros, those bytes come back as zeros, the way a filesystem
// can extend a file before its data blocks land. The result is durable.
func (c *CrashFile) LosePower(tear int, zeros bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var extra []byte
	if len(c.data) > len(c.durable) {
		extra = c.data[len(c.durable):]
	}
	extra = extra[:min(tear, len(extra))]
	if zeros {
		extra = make([]byte, len(extra))
	}
	c.data = append(append([]byte(nil), c.durable...), extra...)
	c.durable = append([]byte(nil), c.data...)
}

func (c *CrashFile) ReadAt(p []byte, off int64) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if off >= int64(len(c.data)) {
		return 0, io.EOF
	}
	n := copy(p, c.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (c *CrashFile) WriteAt(p []byte, off int64) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.FailWrite != nil {
		return 0, c.FailWrite
	}
	if end := off + int64(len(p)); end > int64(len(c.data)) {
		c.data = append(c.data, make([]byte, end-int64(len(c.data)))...)
	}
	copy(c.data[off:], p)
	return len(p), nil
}

func (c *CrashFile) Truncate(size int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if size <= int64(len(c.data)) {
		c.data = c.data[:size:size]
	} else {
		c.data = append(c.data, make([]byte, size-int64(len(c.data)))...)
	}
	return nil
}

func (c *CrashFile) Sync() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.FailSync != nil {
		return c.FailSync
	}
	c.durable = append([]byte(nil), c.data...)
	return nil
}

func (c *CrashFile) Size() (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return int64(len(c.data)), nil
}
