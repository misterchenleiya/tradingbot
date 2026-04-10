package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type OHLCVRetentionTarget struct {
	Exchange  string
	Symbol    string
	Timeframe string
}

type OHLCVRetentionService struct {
	store   *SQLite
	policy  HistoryPolicy
	logger  *zap.Logger
	started atomic.Bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

func NewOHLCVRetentionService(store *SQLite, policy HistoryPolicy, logger *zap.Logger) *OHLCVRetentionService {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &OHLCVRetentionService{
		store:  store,
		policy: policy,
		logger: logger,
	}
}

func (s *OHLCVRetentionService) Start(ctx context.Context) error {
	if s == nil {
		return errors.New("nil ohlcv retention service")
	}
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("ohlcv retention service already started")
	}
	if !s.policy.Enabled || !s.policy.Cleanup.Enabled {
		return nil
	}
	if s.store == nil {
		s.started.Store(false)
		return errors.New("nil storage")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.run(runCtx)
	}()
	return nil
}

func (s *OHLCVRetentionService) Close() error {
	if s == nil {
		return nil
	}
	if !s.started.CompareAndSwap(true, false) {
		return nil
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.cancel = nil
	return nil
}

func (s *OHLCVRetentionService) SetLogger(logger *zap.Logger) {
	if s == nil {
		return
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	s.logger = logger
}

func (s *OHLCVRetentionService) run(ctx context.Context) {
	interval := time.Duration(s.policy.Cleanup.IntervalSeconds) * time.Second
	if interval < time.Minute {
		interval = time.Minute
	}
	for {
		if ctx.Err() != nil {
			return
		}
		deleted, err := s.cleanupOnce()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Warn("ohlcv retention cleanup failed", zap.Error(err))
		} else if deleted > 0 {
			s.logger.Info("ohlcv retention cleanup done", zap.Int64("deleted", deleted))
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

func (s *OHLCVRetentionService) cleanupOnce() (int64, error) {
	if s == nil || s.store == nil {
		return 0, nil
	}
	if !s.policy.Enabled || !s.policy.Cleanup.Enabled {
		return 0, nil
	}
	targets, err := s.store.ListOHLCVRetentionTargets()
	if err != nil {
		return 0, err
	}
	if len(targets) == 0 {
		return 0, nil
	}
	var totalDeleted int64
	for _, target := range targets {
		maxBars, limited := s.policy.MaxBarsFor(target.Exchange, target.Symbol)
		if !limited || maxBars <= 0 {
			continue
		}
		deleted, deleteErr := s.store.DeleteOHLCVKeepLatestBars(target.Exchange, target.Symbol, target.Timeframe, maxBars)
		if deleteErr != nil {
			s.logger.Warn("ohlcv retention delete failed",
				zap.String("exchange", target.Exchange),
				zap.String("symbol", target.Symbol),
				zap.String("timeframe", target.Timeframe),
				zap.Int64("max_history_bars", maxBars),
				zap.Error(deleteErr),
			)
			continue
		}
		totalDeleted += deleted
	}
	return totalDeleted, nil
}

func (s *SQLite) ListOHLCVRetentionTargets() (out []OHLCVRetentionTarget, err error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("nil db")
	}
	rows, err := s.DB.Query(
		`SELECT DISTINCT e.name, sy.symbol, o.timeframe
		   FROM ohlcv o
		   JOIN exchanges e ON e.id = o.exchange_id
		   JOIN symbols sy ON sy.id = o.symbol_id
		  ORDER BY e.name, sy.symbol, o.timeframe;`,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				err = fmt.Errorf("close rows: %v; %w", closeErr, err)
			}
		}
	}()
	for rows.Next() {
		var item OHLCVRetentionTarget
		if scanErr := rows.Scan(&item.Exchange, &item.Symbol, &item.Timeframe); scanErr != nil {
			return nil, scanErr
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLite) DeleteOHLCVKeepLatestBars(exchange, symbol, timeframe string, keepBars int64) (int64, error) {
	if s == nil || s.DB == nil {
		return 0, fmt.Errorf("nil db")
	}
	if keepBars <= 0 {
		return 0, nil
	}
	exchangeID, symbolID, err := s.lookupSymbolIDs(exchange, symbol)
	if err != nil {
		return 0, err
	}
	var cutoffTS int64
	err = s.DB.QueryRow(
		`SELECT ts
		   FROM ohlcv
		  WHERE exchange_id = ? AND symbol_id = ? AND timeframe = ?
		  ORDER BY ts DESC
		  LIMIT 1 OFFSET ?;`,
		exchangeID, symbolID, timeframe, keepBars-1,
	).Scan(&cutoffTS)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if cutoffTS <= 0 {
		return 0, nil
	}
	const batchSize = 5000
	var total int64
	for {
		res, execErr := s.DB.Exec(
			`DELETE FROM ohlcv
			  WHERE exchange_id = ? AND symbol_id = ? AND timeframe = ?
			    AND ts IN (
			      SELECT ts
			        FROM ohlcv
			       WHERE exchange_id = ? AND symbol_id = ? AND timeframe = ? AND ts < ?
			       ORDER BY ts ASC
			       LIMIT ?
			    );`,
			exchangeID, symbolID, timeframe,
			exchangeID, symbolID, timeframe, cutoffTS, batchSize,
		)
		if execErr != nil {
			return total, execErr
		}
		affected, rowsErr := res.RowsAffected()
		if rowsErr != nil {
			return total, rowsErr
		}
		if affected <= 0 {
			return total, nil
		}
		total += affected
		if affected < batchSize {
			return total, nil
		}
	}
}
