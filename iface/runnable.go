package iface

import "context"

type Runnable interface {
	Start(ctx context.Context) error
}
