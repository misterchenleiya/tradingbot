package singleton

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/misterchenleiya/tradingbot/storage"
	"go.uber.org/zap"
)

type ServiceConfig struct {
	Store   *storage.SQLite
	TTL     time.Duration
	Version string
	Mode    string
	Source  string
	Logger  *zap.Logger
}

type Service struct {
	cfg     ServiceConfig
	manager *Manager
	Lock    *Lock
	Status  string

	started atomic.Bool
	stopCh  chan struct{}
	doneCh  chan struct{}
	errCh   chan error
	mu      sync.Mutex
}

func NewService(cfg ServiceConfig) *Service {
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	cfg.Logger = logger
	return &Service{
		cfg:   cfg,
		errCh: make(chan error, 1),
	}
}

func (s *Service) Errors() <-chan error {
	if s == nil {
		return nil
	}
	return s.errCh
}

func (s *Service) SetLogger(logger *zap.Logger) {
	if s == nil {
		return
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	s.cfg.Logger = logger
}

func (s *Service) SetStatus(status string) {
	if s == nil {
		return
	}
	s.Status = status
}

func (s *Service) Start(ctx context.Context) (err error) {
	if s == nil {
		return errors.New("nil singleton service")
	}
	logger := s.cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	fields := []zap.Field{
		zap.String("source", s.cfg.Source),
		zap.Duration("ttl", s.cfg.TTL),
		zap.String("version", s.cfg.Version),
	}
	logger.Info("singleton start", fields...)
	defer func() {
		logger.Info("singleton started")
	}()
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("singleton already started")
	}
	if s.cfg.Store == nil || s.cfg.Store.DB == nil {
		s.started.Store(false)
		return errors.New("nil store")
	}
	if err := EnsureTable(s.cfg.Store.DB); err != nil {
		s.started.Store(false)
		return err
	}
	manager := NewManager(s.cfg.Store.DB, s.cfg.TTL)
	lock, err := manager.Acquire(s.cfg.Version, s.cfg.Mode, s.cfg.Source)
	if err != nil {
		s.started.Store(false)
		return err
	}
	s.manager = manager
	s.Lock = lock
	s.stopCh = make(chan struct{})
	s.doneCh = make(chan struct{})
	if ctx == nil {
		ctx = context.Background()
	}
	go s.runHeartbeat(ctx)
	return nil
}

func (s *Service) Close() (err error) {
	if s == nil {
		return nil
	}
	logger := s.cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	logger.Info("singleton close")
	defer func() {
		logger.Info("singleton closed")
	}()
	if !s.started.CompareAndSwap(true, false) {
		return nil
	}
	s.stopHeartbeat()
	status := s.Status
	if status == "" {
		status = StatusCompleted
	}
	if s.manager != nil {
		return s.manager.Release(status)
	}
	return nil
}

func (s *Service) runHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.TTL / 3)
	defer ticker.Stop()
	defer close(s.doneCh)
	for {
		select {
		case <-ticker.C:
			if s.manager == nil {
				return
			}
			if err := s.manager.Heartbeat(); err != nil {
				s.sendErr(err)
				return
			}
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		}
	}
}

func (s *Service) stopHeartbeat() {
	s.mu.Lock()
	stopCh := s.stopCh
	doneCh := s.doneCh
	s.stopCh = nil
	s.doneCh = nil
	s.mu.Unlock()
	if stopCh != nil {
		close(stopCh)
	}
	if doneCh != nil {
		<-doneCh
	}
}

func (s *Service) sendErr(err error) {
	if err == nil {
		return
	}
	select {
	case s.errCh <- err:
	default:
	}
}
