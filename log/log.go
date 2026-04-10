package log

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Config struct {
	Level        string
	Console      bool
	File         bool
	FilePath     string
	LinkPath     string
	MaxSizeMB    int
	MaxBackups   int
	MaxAgeDays   int
	RotateHours  int
	Compress     bool
	Development  bool
	EncodeTimeFn zapcore.TimeEncoder
}

func New(cfg Config) (*zap.Logger, error) {
	level := zapcore.InfoLevel
	if cfg.Level != "" {
		if err := level.Set(cfg.Level); err != nil {
			return nil, err
		}
	}

	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "ts"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	if cfg.EncodeTimeFn != nil {
		encoderCfg.EncodeTime = cfg.EncodeTimeFn
	}

	var cores []zapcore.Core
	encoder := zapcore.NewJSONEncoder(encoderCfg)

	if cfg.Console {
		console := safeSyncer{WriteSyncer: zapcore.AddSync(os.Stdout), ignoreENOTTY: true}
		cores = append(cores, zapcore.NewCore(encoder, console, level))
	}
	if cfg.File {
		if err := ensureParentDir(cfg.FilePath); err != nil {
			return nil, err
		}
		if err := ensureParentDir(cfg.LinkPath); err != nil {
			return nil, err
		}
		if cfg.LinkPath != "" {
			if err := ensureSymlink(cfg.LinkPath, cfg.FilePath); err != nil {
				return nil, err
			}
		}
		fileWriter := &lumberjack.Logger{
			Filename:   cfg.FilePath,
			MaxSize:    cfg.MaxSizeMB,
			MaxBackups: cfg.MaxBackups,
			MaxAge:     cfg.MaxAgeDays,
			Compress:   cfg.Compress,
		}
		var writer zapcore.WriteSyncer = zapcore.AddSync(fileWriter)
		if cfg.RotateHours > 0 {
			rotating, err := newTimeRotatingWriter(fileWriter, cfg)
			if err != nil {
				return nil, err
			}
			writer = zapcore.AddSync(rotating)
		}
		cores = append(cores, zapcore.NewCore(encoder, writer, level))
	}
	if len(cores) == 0 {
		cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(io.Discard), level))
	}

	core := zapcore.NewTee(cores...)
	if cfg.Development {
		return zap.New(core, zap.AddCaller(), zap.Development()), nil
	}
	return zap.New(core, zap.AddCaller()), nil
}

func ensureParentDir(path string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	dir := filepath.Dir(path)
	if strings.TrimSpace(dir) == "" || dir == "." {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create log directory failed: %w", err)
	}
	return nil
}

func Nop() *zap.Logger {
	return zap.NewNop()
}

type safeSyncer struct {
	zapcore.WriteSyncer
	ignoreENOTTY bool
}

func (s safeSyncer) Sync() error {
	if s.WriteSyncer == nil {
		return nil
	}
	err := s.WriteSyncer.Sync()
	if err == nil {
		return nil
	}
	if s.ignoreENOTTY && errors.Is(err, syscall.ENOTTY) {
		return nil
	}
	return err
}

func ensureSymlink(linkPath, targetPath string) error {
	if linkPath == "" {
		return nil
	}
	if targetPath == "" {
		return fmt.Errorf("empty log file path")
	}

	info, err := os.Lstat(linkPath)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			if err := os.Remove(linkPath); err != nil {
				return err
			}
		} else {
			backup := fmt.Sprintf("%s.%s.bak", linkPath, time.Now().In(time.Local).Format("20060102_150405"))
			if err := os.Rename(linkPath, backup); err != nil {
				return err
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	target := targetPath
	if filepath.Dir(linkPath) == filepath.Dir(targetPath) {
		target = filepath.Base(targetPath)
	}
	return os.Symlink(target, linkPath)
}

type timeRotatingWriter struct {
	mu           sync.Mutex
	logger       *lumberjack.Logger
	rotateEvery  time.Duration
	nextRotation time.Time
	logDir       string
	prefix       string
	ext          string
	linkPath     string
	maxSizeMB    int
	maxBackups   int
	maxAgeDays   int
	compress     bool
	lastCleanup  time.Time
}

func newTimeRotatingWriter(logger *lumberjack.Logger, cfg Config) (*timeRotatingWriter, error) {
	if logger == nil {
		return nil, fmt.Errorf("nil logger")
	}
	rotateEvery := time.Duration(cfg.RotateHours) * time.Hour
	if rotateEvery <= 0 {
		return nil, fmt.Errorf("invalid rotate hours: %d", cfg.RotateHours)
	}

	logDir, prefix, ext, err := resolveLogParts(cfg)
	if err != nil {
		return nil, err
	}

	now := time.Now().In(time.Local)
	writer := &timeRotatingWriter{
		logger:       logger,
		rotateEvery:  rotateEvery,
		nextRotation: now.Add(rotateEvery),
		logDir:       logDir,
		prefix:       prefix,
		ext:          ext,
		linkPath:     cfg.LinkPath,
		maxSizeMB:    cfg.MaxSizeMB,
		maxBackups:   cfg.MaxBackups,
		maxAgeDays:   cfg.MaxAgeDays,
		compress:     cfg.Compress,
	}
	writer.cleanupOld(now)
	return writer, nil
}

func (w *timeRotatingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now().In(time.Local)
	if now.After(w.nextRotation) {
		if err := w.rotate(now); err != nil {
			n, writeErr := w.logger.Write(p)
			if writeErr != nil {
				return n, fmt.Errorf("rotate failed: %v; write failed: %w", err, writeErr)
			}
			return n, fmt.Errorf("rotate failed: %w", err)
		}
	}

	return w.logger.Write(p)
}

func (w *timeRotatingWriter) rotate(now time.Time) error {
	if err := w.logger.Close(); err != nil {
		return err
	}
	filePath := filepath.Join(w.logDir, formatLogFileName(w.prefix, w.ext, now))
	w.logger = &lumberjack.Logger{
		Filename:   filePath,
		MaxSize:    w.maxSizeMB,
		MaxBackups: w.maxBackups,
		MaxAge:     w.maxAgeDays,
		Compress:   w.compress,
	}
	if w.linkPath != "" {
		if err := ensureSymlink(w.linkPath, filePath); err != nil {
			return err
		}
	}
	w.nextRotation = now.Add(w.rotateEvery)
	w.cleanupOld(now)
	return nil
}

func (w *timeRotatingWriter) cleanupOld(now time.Time) {
	if w.maxAgeDays <= 0 {
		return
	}
	if !w.lastCleanup.IsZero() && now.Sub(w.lastCleanup) < time.Hour {
		return
	}
	cutoff := now.Add(-time.Duration(w.maxAgeDays) * 24 * time.Hour)
	entries, err := os.ReadDir(w.logDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ts, ok := parseTimestamp(entry.Name(), w.prefix, w.ext)
		if !ok {
			continue
		}
		if ts.Before(cutoff) {
			_ = os.Remove(filepath.Join(w.logDir, entry.Name()))
		}
	}
	w.lastCleanup = now
}

func resolveLogParts(cfg Config) (string, string, string, error) {
	if cfg.LinkPath != "" {
		return parseLogParts(cfg.LinkPath)
	}
	if cfg.FilePath == "" {
		return "", "", "", fmt.Errorf("empty log file path")
	}
	return inferLogPartsFromFile(cfg.FilePath)
}

func parseLogParts(path string) (string, string, string, error) {
	ext := filepath.Ext(path)
	if ext == "" {
		return "", "", "", fmt.Errorf("log file extension missing: %s", path)
	}
	base := filepath.Base(path)
	prefix := strings.TrimSuffix(base, ext)
	if prefix == "" {
		return "", "", "", fmt.Errorf("log file prefix missing: %s", path)
	}
	return filepath.Dir(path), prefix, ext, nil
}

func inferLogPartsFromFile(path string) (string, string, string, error) {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	ext := filepath.Ext(base)
	if ext == "" {
		return "", "", "", fmt.Errorf("log file extension missing: %s", path)
	}
	name := strings.TrimSuffix(base, ext)
	parts := strings.Split(name, "_")
	if len(parts) >= 3 {
		if _, err := time.ParseInLocation("2006-01-02_150405", parts[0]+"_"+parts[1], time.Local); err == nil {
			prefix := strings.Join(parts[2:], "_")
			if prefix == "" {
				return "", "", "", fmt.Errorf("log file prefix missing: %s", path)
			}
			return dir, prefix, ext, nil
		}
		tsParts := parts[len(parts)-2:]
		if isCompactTimestampParts(tsParts) {
			prefix := strings.Join(parts[:len(parts)-2], "_")
			if prefix == "" {
				return "", "", "", fmt.Errorf("log file prefix missing: %s", path)
			}
			return dir, prefix, ext, nil
		}
	}
	return "", "", "", fmt.Errorf("log file name timestamp invalid: %s", path)
}

func isCompactTimestampParts(parts []string) bool {
	if len(parts) != 2 {
		return false
	}
	lengths := []int{8, 6}
	for i, part := range parts {
		if len(part) != lengths[i] {
			return false
		}
		if _, err := strconv.Atoi(part); err != nil {
			return false
		}
	}
	return true
}

func formatLogFileName(prefix, ext string, now time.Time) string {
	ts := now.In(time.Local).Format("2006-01-02_150405")
	return fmt.Sprintf("%s_%s%s", ts, prefix, ext)
}

func parseTimestamp(name, prefix, ext string) (time.Time, bool) {
	if strings.HasPrefix(name, prefix+"_") {
		base := name
		if strings.HasSuffix(base, ext) {
			base = strings.TrimSuffix(base, ext)
		} else if idx := strings.Index(base, ext+"."); idx >= 0 {
			base = base[:idx]
		} else {
			return time.Time{}, false
		}
		tsPart := strings.TrimPrefix(base, prefix+"_")
		ts, err := time.ParseInLocation("20060102_150405", tsPart, time.Local)
		if err != nil {
			return time.Time{}, false
		}
		return ts, true
	}
	base := name
	if strings.HasSuffix(base, ext) {
		base = strings.TrimSuffix(base, ext)
	} else if idx := strings.Index(base, ext+"."); idx >= 0 {
		base = base[:idx]
	} else {
		return time.Time{}, false
	}
	tsPart, ok := strings.CutSuffix(base, "_"+prefix)
	if !ok {
		return time.Time{}, false
	}
	ts, err := time.ParseInLocation("2006-01-02_150405", tsPart, time.Local)
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}
