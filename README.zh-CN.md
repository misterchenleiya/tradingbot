# tradingbot

[English](./README.md) | 简体中文

`tradingbot` 是一个从更大闭源量化交易系统中选择性导出的 MIT 开源仓库。  
当前公开范围包括运行时主链路、基于 SQLite 的存储层、行情/执行/风控模块、嵌入式导出前端，以及仅保留的 `turtle` 策略实现。

## 界面截图

### TradingView 导出页

![TradingView exporter screenshot](./tradingview.png)

### Bubbles 导出页

![Bubbles exporter screenshot](./bubbles.png)

## 项目概览

- 这是一个基于 Go 的量化交易程序公开基线仓库，使用 SQLite 持久化，并内嵌前端导出页面。
- 当前支持的运行模式包括 `init`、`live`、`paper`、`back-test`、`sql`、`reset-cooldown`。
- 当前公开策略只保留 `turtle`。
- 当前公开的前端导出页包括 `bubbles`、`history`、`tradingview`。
- 仓库中的内部域名已经统一替换为 `example.com` 占位地址。

## 当前公开内容

当前仓库包含：

- `app/`：程序入口和运行模式装配
- `core/`：核心调度、快照组装、回测/实盘驱动
- `exchange/`：交易所与行情抽象
- `execution/`、`risk/`：执行和风控链路
- `storage/`：SQLite schema、默认配置和运行态持久化
- `exporter/`：导出接口和前端页面
- `strategy/turtle/`：当前公开策略

当前仓库不包含：

- 私有策略目录，例如 `strategy/elder/`、`strategy/simpleelder/`
- 闭源仓库中的内部设计文档和大部分 Markdown 文档
- 不属于当前公开运行时基线的内部工具目录

## 目录说明

| 路径 | 作用 |
| --- | --- |
| `app/` | 程序入口和运行模式装配 |
| `common/` | 通用工具和传输辅助 |
| `core/` | 策略评估、快照组装、实盘/回测主驱动 |
| `exchange/` | 交易所配置、行情适配和运行时装配 |
| `execution/` | 回测与实盘执行链路 |
| `exporter/` | 嵌入式 HTTP/WebSocket 导出接口和前端应用 |
| `iface/` | 跨模块接口定义 |
| `internal/models/` | 共享领域模型 |
| `log/` | Zap + lumberjack 日志封装 |
| `risk/` | 信号生命周期、趋势保护、开平仓过滤 |
| `singleton/` | 单实例租约与心跳 |
| `storage/` | SQLite schema、默认配置和持久化 |
| `strategy/turtle/` | 当前公开策略实现 |
| `ta/` | 技术指标辅助 |
| `third_party/go-talib/` | 仓库内置 TA-Lib 兼容依赖 |

## 环境要求

- Go `1.22+`
- 如需重建前端页面，需要 Node.js 和 npm
- 本地 SQLite 原生构建需要可用的 CGO 工具链
- 在 macOS 上执行 `make linux` 时，建议准备 Docker 或 Linux 交叉编译器

## 快速开始

### 1. 初始化数据库

```bash
go run app/main.go --mode=init
```

程序默认使用 `./gobot.db`，也可以通过 `-db` 自定义路径。

### 2. 以模拟盘模式运行

```bash
go run app/main.go --mode=paper
```

或者使用 Makefile 入口：

```bash
make run
```

### 3. 执行一次回测

```bash
go run app/main.go --mode=back-test -source=exchange:okx:btcusdtp:15m/1h:20260101_1200-20260115_1600
```

当前支持的回测数据源：

- `exchange:...`
- `db:...`
- `csv:...`

### 4. 构建当前平台产物

```bash
make build
```

该命令会把当前平台的二进制和运行脚本输出到 `build/`。

### 5. 构建 Linux 产物

```bash
make linux
```

### 6. 打包发布产物

```bash
make pack
```

## 前端导出页

仓库内包含三个嵌入式前端应用：

- `exporter/bubbles`
- `exporter/history`
- `exporter/tradingview`

如果需要手动重建：

```bash
npm -C exporter/bubbles run build
```

```bash
npm -C exporter/history run build
```

```bash
npm -C exporter/tradingview run build
```

## 运行模式

| 模式 | 说明 |
| --- | --- |
| `init` | 初始化 schema 和默认运行配置 |
| `live` | 实盘运行模式 |
| `paper` | 模拟盘运行模式 |
| `back-test` | 历史回放与回测汇总 |
| `sql` | 对 SQLite 数据库执行临时 SQL |
| `reset-cooldown` | 重置当前交易日的冷却记录 |

## 配置说明

- 运行时配置保存在 SQLite 中，第一次使用需要先执行 `-mode=init`
- 当前默认策略配置只启用 `turtle`
- 默认导出服务地址为 `http://127.0.0.1:8081`
- 仓库中的 `example.com` 是公开版占位地址，fork 后请按自己的部署环境替换

## 开源说明

- 当前仓库是公开导出基线，不等于完整闭源系统
- 导出时保留运行时主链路，移除了私有策略和内部文档
- 许可证见 [LICENSE](./LICENSE)
