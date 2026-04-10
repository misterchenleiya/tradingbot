module github.com/misterchenleiya/tradingbot

go 1.22

require (
	github.com/google/uuid v1.6.0
	github.com/markcheno/go-talib v0.0.0-20250114000313-ec55a20c902f
	github.com/mattn/go-sqlite3 v1.14.22
	go.uber.org/zap v1.27.0
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
	nhooyr.io/websocket v1.8.7
)

require (
	github.com/klauspost/compress v1.10.3 // indirect
	go.uber.org/multierr v1.10.0 // indirect
)

replace github.com/markcheno/go-talib => ./third_party/go-talib
