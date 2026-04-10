package log

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestInferLogPartsFromFileSupportsOldAndNewFormats(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "new format",
			path: filepath.Join("/tmp", "2026-03-30_153045_gobot.log"),
			want: "gobot",
		},
		{
			name: "old format",
			path: filepath.Join("/tmp", "gobot_20260330_153045.log"),
			want: "gobot",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, prefix, ext, err := inferLogPartsFromFile(tt.path)
			if err != nil {
				t.Fatalf("inferLogPartsFromFile() error = %v", err)
			}
			if dir != "/tmp" {
				t.Fatalf("inferLogPartsFromFile() dir = %q want %q", dir, "/tmp")
			}
			if prefix != tt.want {
				t.Fatalf("inferLogPartsFromFile() prefix = %q want %q", prefix, tt.want)
			}
			if ext != ".log" {
				t.Fatalf("inferLogPartsFromFile() ext = %q want %q", ext, ".log")
			}
		})
	}
}

func TestFormatLogFileNameUsesDatePrefix(t *testing.T) {
	got := formatLogFileName("gobot", ".log", time.Date(2026, 3, 30, 15, 30, 45, 0, time.Local))
	want := "2026-03-30_153045_gobot.log"
	if got != want {
		t.Fatalf("formatLogFileName() = %q want %q", got, want)
	}
}

func TestParseTimestampSupportsOldAndNewFormats(t *testing.T) {
	tests := []struct {
		name string
		file string
	}{
		{
			name: "new format",
			file: "2026-03-30_153045_gobot.log",
		},
		{
			name: "old format",
			file: "gobot_20260330_153045.log",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseTimestamp(tt.file, "gobot", ".log")
			if !ok {
				t.Fatalf("parseTimestamp() ok = false")
			}
			want := time.Date(2026, 3, 30, 15, 30, 45, 0, time.Local)
			if !got.Equal(want) {
				t.Fatalf("parseTimestamp() = %v want %v", got, want)
			}
		})
	}
}

func TestNewCreatesMissingLogDirectory(t *testing.T) {
	tempDir := t.TempDir()
	logDir := filepath.Join(tempDir, "logs")
	filePath := filepath.Join(logDir, "2026-03-30_153045_gobot.log")
	linkPath := filepath.Join(logDir, "gobot.log")

	logger, err := New(Config{
		Level:    "info",
		Console:  false,
		File:     true,
		FilePath: filePath,
		LinkPath: linkPath,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() {
		_ = logger.Sync()
	}()

	if _, err := os.Stat(logDir); err != nil {
		t.Fatalf("expected log directory created, stat error = %v", err)
	}
	info, err := os.Lstat(linkPath)
	if err != nil {
		t.Fatalf("expected symlink created, lstat error = %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("expected %s to be symlink", linkPath)
	}
}
