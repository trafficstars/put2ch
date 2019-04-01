package udp2ch

import (
	"bytes"
	"sync"
)

type buffer struct {
	bytes.Buffer
}

var (
	bufferPool = sync.Pool{
		New: func() interface{} {
			return &buffer{}
		},
	}
)

func newBuffer() *buffer {
	return bufferPool.Get().(*buffer)
}

func (buf *buffer) Release() {
	buf.Reset()
	bufferPool.Put(buf)
}
