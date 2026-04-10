package iface

import "io"

type Closeable interface {
	io.Closer
}
