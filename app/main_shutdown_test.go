package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

type testService struct {
	closeFn func() error
}

func (s *testService) Start(context.Context) error {
	return nil
}

func (s *testService) Close() error {
	if s.closeFn != nil {
		return s.closeFn()
	}
	return nil
}

func TestCloseServicesWithinTimeoutClosesInReverseOrder(t *testing.T) {
	var (
		mu    sync.Mutex
		order []string
	)
	services := []namedService{
		{
			name: "first",
			svc: &testService{closeFn: func() error {
				mu.Lock()
				order = append(order, "first")
				mu.Unlock()
				return nil
			}},
		},
		{
			name: "second",
			svc: &testService{closeFn: func() error {
				mu.Lock()
				order = append(order, "second")
				mu.Unlock()
				return nil
			}},
		},
	}

	if ok := closeServicesWithinTimeout(services, zap.NewNop(), 100*time.Millisecond, nil, "runtime"); !ok {
		t.Fatalf("expected shutdown to complete within timeout")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 {
		t.Fatalf("unexpected close count: got %d want 2", len(order))
	}
	if order[0] != "second" || order[1] != "first" {
		t.Fatalf("unexpected close order: got %v want [second first]", order)
	}
}

func TestCloseServicesWithinTimeoutReturnsFalseOnTimeout(t *testing.T) {
	blockCh := make(chan struct{})
	closedCh := make(chan struct{})
	services := []namedService{
		{
			name: "slow",
			svc: &testService{closeFn: func() error {
				defer close(closedCh)
				<-blockCh
				return nil
			}},
		},
	}

	if ok := closeServicesWithinTimeout(services, zap.NewNop(), 20*time.Millisecond, nil, "runtime"); ok {
		t.Fatalf("expected shutdown timeout to return false")
	}

	close(blockCh)
	select {
	case <-closedCh:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for blocked close goroutine to finish")
	}
}

func TestCloseServicesWithinTimeoutReturnsFalseOnForceSignal(t *testing.T) {
	blockCh := make(chan struct{})
	closedCh := make(chan struct{})
	forceCh := make(chan struct{})
	services := []namedService{
		{
			name: "slow",
			svc: &testService{closeFn: func() error {
				defer close(closedCh)
				<-blockCh
				return nil
			}},
		},
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		close(forceCh)
	}()

	if ok := closeServicesWithinTimeout(services, zap.NewNop(), time.Second, forceCh, "runtime"); ok {
		t.Fatalf("expected force shutdown to interrupt close wait")
	}

	close(blockCh)
	select {
	case <-closedCh:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for blocked close goroutine to finish")
	}
}
