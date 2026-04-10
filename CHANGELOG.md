# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initialized the repository with an empty `README.md`.
- Added the first public code sync baseline with `app`, `common`, `core`, `exchange`, `execution`, `exporter`, `iface`, `internal`, `log`, `risk`, `singleton`, `storage`, `strategy/turtle`, `ta`, and `third_party/go-talib`.
- Added an MIT `LICENSE` for the public repository.
- Added a bilingual README setup with English-first [README.md](/Users/chenlei/Codes/www/github.com/07c2/tradingbot/README.md), Chinese [README.zh-CN.md](/Users/chenlei/Codes/www/github.com/07c2/tradingbot/README.zh-CN.md), and homepage screenshots for `tradingview` and `bubbles`.

### Changed
- Sanitized internal module paths and service domains for the public repository.
- Reduced the public strategy set to `turtle` in registry and default runtime config.
