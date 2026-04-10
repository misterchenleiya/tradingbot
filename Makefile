tag := $(shell git describe --tags --exact-match 2>/dev/null || echo "")
commit := $(shell git rev-parse --short HEAD 2>/dev/null)
build_time := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
MODULE = "github.com/misterchenleiya/tradingbot"
GO_BIN := $(shell \
	for candidate in "$$(command -v go 2>/dev/null)" "$$HOME/.goenv/versions/1.23.12/bin/go" "$$HOME/.goenv/versions/1.22.0/bin/go" /opt/homebrew/bin/go /usr/local/go/bin/go /opt/homebrew/opt/go/libexec/bin/go; do \
		if [ -n "$$candidate" ] && [ -x "$$candidate" ] && "$$candidate" version >/dev/null 2>&1; then \
			printf "%s" "$$candidate"; \
			break; \
		fi; \
	done)

PROJECT_NAME ?= $(notdir $(abspath $(CURDIR)))
ARTIFACT_PREFIX ?= $(PROJECT_NAME)
PUBLISH_PATH ?= $(PROJECT_NAME)
UPLOAD_BASE ?=
UPLOAD_USERNAME ?=
UPLOAD_PASSWORD ?=
BUILD_DIR ?= ./build
BINARY_SYMLINK ?= ./$(ARTIFACT_PREFIX)
UPDATE_SCRIPT_SRC ?= ./dev/update.sh
START_SCRIPT_SRC ?= ./dev/start.sh
STOP_SCRIPT_SRC ?= ./dev/stop.sh
RESTART_SCRIPT_SRC ?= ./dev/restart.sh
STATUS_SCRIPT_SRC ?= ./dev/status.sh
BINARY_FILE ?= $(ARTIFACT_PREFIX)_$(commit)
ARCHIVE_FILE ?= $(ARTIFACT_PREFIX)_$(commit).tar.gz
LATEST_FILE ?= $(BUILD_DIR)/latest.txt
RUNTIME_SCRIPT_FILES ?= update.sh start.sh stop.sh restart.sh status.sh
PACK_FILES ?= $(BINARY_FILE) $(RUNTIME_SCRIPT_FILES)
CURL_UPLOAD_ARGS ?= --fail --show-error --max-time 30
UPLOAD_RETRY_MAX ?= 5
UPLOAD_RETRY_DELAY ?= 1

SHA := $(firstword \
	$(shell command -v sha256sum 2>/dev/null) \
	$(shell command -v gsha256sum 2>/dev/null) \
	$(shell command -v shasum 2>/dev/null))
ifeq ($(notdir $(SHA)),shasum)
SHAFLAGS := -a 256
else
SHAFLAGS :=
endif

ifndef SHA
$(error No sha256 tool found. Install coreutils (sha256sum) or use shasum)
endif

ifndef GO_BIN
$(error No working go binary found. Fix the local Go installation or set GO_BIN explicitly)
endif

LD_FLAGS := -ldflags "-w -s \
	-X github.com/misterchenleiya/tradingbot/common.Tag=$(tag) \
	-X github.com/misterchenleiya/tradingbot/common.Commit=$(commit) \
	-X github.com/misterchenleiya/tradingbot/common.BuildTime=$(build_time)"

LINUX_CC ?= $(firstword \
	$(shell command -v x86_64-linux-gnu-gcc 2>/dev/null) \
	$(shell command -v x86_64-linux-musl-gcc 2>/dev/null))
DOCKER_BIN ?= $(shell command -v docker 2>/dev/null)
DOCKER_IMAGE ?= golang:1.22-bookworm

.PHONY: init run build linux clean clean-runtime pack write-latest upload publish bubbles history tradingview prepare-runtime-files verify-commit

BUBBLES_DIR := ./exporter/bubbles
BUBBLES_BUILD_DIR := $(BUBBLES_DIR)/dist
HISTORY_DIR := ./exporter/history
HISTORY_BUILD_DIR := $(HISTORY_DIR)/dist
TRADINGVIEW_DIR := ./exporter/tradingview
TRADINGVIEW_BUILD_DIR := $(TRADINGVIEW_DIR)/dist

define ensure_npm_deps
	@if [ -f "$(1)/package.json" ]; then \
		needs_sync=0; \
		if [ ! -d "$(1)/node_modules" ]; then \
			needs_sync=1; \
		elif [ -f "$(1)/package-lock.json" ] && [ ! -f "$(1)/node_modules/.package-lock.json" ]; then \
			needs_sync=1; \
		elif [ ! -f "$(1)/package-lock.json" ] && [ ! -f "$(1)/node_modules/.package-lock.json" ]; then \
			needs_sync=1; \
		elif [ "$(1)/package.json" -nt "$(1)/node_modules/.package-lock.json" ]; then \
			needs_sync=1; \
		elif [ -f "$(1)/package-lock.json" ] && [ "$(1)/package-lock.json" -nt "$(1)/node_modules/.package-lock.json" ]; then \
			needs_sync=1; \
		fi; \
		if [ "$$needs_sync" -eq 1 ]; then \
			echo "sync $(2) dependencies..."; \
			if [ -f "$(1)/package-lock.json" ]; then \
				npm -C "$(1)" ci; \
			else \
				npm -C "$(1)" install; \
			fi; \
		fi; \
	fi
endef

bubbles:
	$(call ensure_npm_deps,$(BUBBLES_DIR),bubbles)
	VITE_DATA_SOURCE=restws npm -C "$(BUBBLES_DIR)" run build

history:
	$(call ensure_npm_deps,$(HISTORY_DIR),history)
	@if [ -f "$(HISTORY_DIR)/package.json" ]; then \
		npm -C "$(HISTORY_DIR)" run build; \
	fi

tradingview:
	$(call ensure_npm_deps,$(TRADINGVIEW_DIR),tradingview)
	@if [ -f "$(TRADINGVIEW_DIR)/package.json" ]; then \
		npm -C "$(TRADINGVIEW_DIR)" run build; \
	fi

prepare-runtime-files:
	@mkdir -p "$(BUILD_DIR)"
	@if [ ! -f "$(UPDATE_SCRIPT_SRC)" ]; then \
		echo "ERROR: $(UPDATE_SCRIPT_SRC) not found."; \
		exit 1; \
	fi
	@if [ ! -f "$(START_SCRIPT_SRC)" ]; then \
		echo "ERROR: $(START_SCRIPT_SRC) not found."; \
		exit 1; \
	fi
	@if [ ! -f "$(STOP_SCRIPT_SRC)" ]; then \
		echo "ERROR: $(STOP_SCRIPT_SRC) not found."; \
		exit 1; \
	fi
	@if [ ! -f "$(RESTART_SCRIPT_SRC)" ]; then \
		echo "ERROR: $(RESTART_SCRIPT_SRC) not found."; \
		exit 1; \
	fi
	@if [ ! -f "$(STATUS_SCRIPT_SRC)" ]; then \
		echo "ERROR: $(STATUS_SCRIPT_SRC) not found."; \
		exit 1; \
	fi
	@rm -f "$(BUILD_DIR)/docker-start.sh" "$(BUILD_DIR)/docker-stop.sh" "$(BUILD_DIR)/docker-restart.sh"
	@cp -f "$(UPDATE_SCRIPT_SRC)" "$(BUILD_DIR)/update.sh"
	@cp -f "$(START_SCRIPT_SRC)" "$(BUILD_DIR)/start.sh"
	@cp -f "$(STOP_SCRIPT_SRC)" "$(BUILD_DIR)/stop.sh"
	@cp -f "$(RESTART_SCRIPT_SRC)" "$(BUILD_DIR)/restart.sh"
	@cp -f "$(STATUS_SCRIPT_SRC)" "$(BUILD_DIR)/status.sh"
	@chmod +x "$(BUILD_DIR)/update.sh" "$(BUILD_DIR)/start.sh" "$(BUILD_DIR)/stop.sh" "$(BUILD_DIR)/restart.sh" "$(BUILD_DIR)/status.sh"

init:
	rm -f ./tradingbot.db
	@if [ -f tradingbot.db ]; then echo "tradingbot.db 已存在"; exit 0; fi
	$(GO_BIN) run app/main.go --mode=init | jq -c "."

run:
	@set -u; \
	tmp_dir="$$(mktemp -d "$${TMPDIR:-/tmp}/$(PROJECT_NAME).run.XXXXXX")"; \
	log_pipe="$$tmp_dir/stdout.pipe"; \
	mkfifo "$$log_pipe"; \
	app_pid=""; \
	output_pid=""; \
	cleanup() { \
		rm -rf "$$tmp_dir"; \
	}; \
	forward_signal() { \
		if [ -n "$$app_pid" ]; then \
			kill -INT "$$app_pid" 2>/dev/null || true; \
		fi; \
	}; \
	trap 'cleanup' EXIT; \
	trap 'forward_signal' INT TERM HUP; \
	if command -v jq >/dev/null 2>&1; then \
		jq -c "." < "$$log_pipe" & \
	else \
		cat "$$log_pipe" & \
	fi; \
	output_pid="$$!"; \
	$(GO_BIN) run app/main.go > "$$log_pipe" & \
	app_pid="$$!"; \
	wait "$$app_pid"; \
	app_status="$$?"; \
	wait "$$output_pid" 2>/dev/null || true; \
	exit "$$app_status"

build: clean bubbles history tradingview
	@mkdir -p $(BUILD_DIR)
	$(GO_BIN) build -v -a $(LD_FLAGS) -o $(BUILD_DIR)/$(BINARY_FILE) app/main.go
	@ln -sfn "$(BUILD_DIR)/$(BINARY_FILE)" "$(BINARY_SYMLINK)"
	@echo "binary symlink updated: $(BINARY_SYMLINK) -> $(BUILD_DIR)/$(BINARY_FILE)"
	@$(MAKE) --no-print-directory prepare-runtime-files

linux: clean bubbles history tradingview
	@mkdir -p $(BUILD_DIR)
	@echo "build linux/amd64 with CGO enabled..."
	@if [ "$$(uname -s)" = "Linux" ]; then \
		CGO_ENABLED=1 GOOS=linux GOARCH=amd64 $(GO_BIN) build -v -a $(LD_FLAGS) -o $(BUILD_DIR)/$(BINARY_FILE) app/main.go; \
	elif [ -n "$(LINUX_CC)" ] && command -v "$(LINUX_CC)" >/dev/null 2>&1; then \
		CGO_ENABLED=1 GOOS=linux GOARCH=amd64 CC="$(LINUX_CC)" $(GO_BIN) build -v -a $(LD_FLAGS) -o $(BUILD_DIR)/$(BINARY_FILE) app/main.go; \
	elif [ -n "$(DOCKER_BIN)" ] && command -v "$(DOCKER_BIN)" >/dev/null 2>&1; then \
		echo "build via docker ($(DOCKER_IMAGE))..."; \
		"$(DOCKER_BIN)" run --rm --platform linux/amd64 \
			-v "$(CURDIR):/src" \
			-w /src \
			"$(DOCKER_IMAGE)" \
			/bin/bash -lc 'set -euo pipefail; \
				if [ -x /usr/local/go/bin/go ]; then export PATH="/usr/local/go/bin:$$PATH"; fi; \
				if ! command -v go >/dev/null 2>&1; then \
					echo "ERROR: go command not found in docker image $(DOCKER_IMAGE)."; \
					exit 127; \
				fi; \
				CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -v -a $(LD_FLAGS) -o $(BUILD_DIR)/$(BINARY_FILE) app/main.go'; \
	else \
		echo "ERROR: go-sqlite3 requires CGO. The current host cannot directly build a usable linux binary."; \
		echo "ERROR: option 1: run make publish on a linux/amd64 machine."; \
		echo "ERROR: option 2: install a linux cross C compiler and rerun with LINUX_CC (e.g. x86_64-linux-gnu-gcc)."; \
		echo "ERROR: option 3: install Docker and rerun make publish (auto docker fallback)."; \
		exit 1; \
	fi
	@$(MAKE) --no-print-directory prepare-runtime-files

clean:
	rm -rf $(BUILD_DIR)

clean-runtime:
	find . -maxdepth 1 -type f \( -name '*.txt' -o -name '*.log' -o -name '*.db' \) -delete

verify-commit:
	@if [ -z "$(strip $(commit))" ]; then \
		echo "ERROR: git commit not found. pack requires a commit-based artifact name."; \
		exit 1; \
	fi

pack: verify-commit prepare-runtime-files
	@echo "pack $(ARCHIVE_FILE) ..."
	@for item in $(PACK_FILES); do \
		if [ ! -e "$(BUILD_DIR)/$$item" ]; then \
			echo "ERROR: $(BUILD_DIR)/$$item not found."; \
			exit 1; \
		fi; \
	done
	@(cd "$(BUILD_DIR)" && tar -czvf "$(ARCHIVE_FILE)" $(PACK_FILES))
	@$(MAKE) --no-print-directory write-latest

write-latest:
	@if [ ! -f "$(BUILD_DIR)/$(ARCHIVE_FILE)" ]; then \
		echo "ERROR: $(BUILD_DIR)/$(ARCHIVE_FILE) not found. Build failed."; \
		exit 1; \
	fi
	@hash_value=`$(SHA) $(SHAFLAGS) "$(BUILD_DIR)/$(ARCHIVE_FILE)" | awk '{print $$1}'`; \
	echo "filename: $(ARCHIVE_FILE)" > "$(LATEST_FILE)"; \
	echo "sha256sum: $$hash_value" >> "$(LATEST_FILE)"; \
	echo "latest.txt generated at $(LATEST_FILE)"
	@(echo "------------------------ latest.txt ------------------------")
	@(cat "$(LATEST_FILE)")
	@(echo "------------------------------------------------------------")

upload:
	@echo "verify upload artifacts ..."
	@if [ ! -f "$(LATEST_FILE)" ]; then \
		echo "ERROR: $(LATEST_FILE) not found."; \
		exit 1; \
	fi
	@artifact_name=`sed -n 's/^filename:[[:space:]]*//p' "$(LATEST_FILE)" | head -n1 | tr -d '\r'`; \
	sha_expected=`sed -n 's/^sha256sum:[[:space:]]*//p' "$(LATEST_FILE)" | head -n1 | tr -d '\r'`; \
	if [ -z "$$artifact_name" ]; then \
		echo "ERROR: filename is missing in $(LATEST_FILE)."; \
		exit 1; \
	fi; \
	if [ -z "$$sha_expected" ]; then \
		echo "ERROR: sha256sum is missing in $(LATEST_FILE)."; \
		exit 1; \
	fi; \
	if [ ! -f "$(BUILD_DIR)/$$artifact_name" ]; then \
		echo "ERROR: $(BUILD_DIR)/$$artifact_name not found."; \
		exit 1; \
	fi; \
	sha_actual=`$(SHA) $(SHAFLAGS) "$(BUILD_DIR)/$$artifact_name" | awk '{print $$1}'`; \
	if [ "$$sha_actual" != "$$sha_expected" ]; then \
		echo "ERROR: sha256 mismatch for $(BUILD_DIR)/$$artifact_name."; \
		echo "ERROR: expected=$$sha_expected actual=$$sha_actual"; \
		exit 1; \
	fi
	@upload_base="$(UPLOAD_BASE)"; \
	upload_username="$(UPLOAD_USERNAME)"; \
	upload_password="$(UPLOAD_PASSWORD)"; \
	if [ -z "$$upload_base" ]; then \
		if [ -t 0 ]; then \
			printf "upload base: "; \
			IFS= read -r upload_base; \
		else \
			echo "ERROR: UPLOAD_BASE is empty."; \
			exit 1; \
		fi; \
	fi; \
	if [ -z "$$upload_base" ]; then \
		echo "ERROR: upload base is empty."; \
		exit 1; \
	fi; \
	if [ -z "$$upload_username" ]; then \
		if [ -t 0 ]; then \
			printf "upload username: "; \
			IFS= read -r upload_username; \
		else \
			echo "ERROR: UPLOAD_USERNAME is empty."; \
			exit 1; \
		fi; \
	fi; \
	if [ -z "$$upload_username" ]; then \
		echo "ERROR: upload username is empty."; \
		exit 1; \
	fi; \
	if [ -z "$$upload_password" ]; then \
		if [ -t 0 ]; then \
			printf "upload password: "; \
			trap 'stty echo' EXIT INT TERM; \
			stty -echo; \
			IFS= read -r upload_password; \
			stty echo; \
			trap - EXIT INT TERM; \
			printf "\n"; \
		else \
			echo "ERROR: UPLOAD_PASSWORD is empty."; \
			exit 1; \
		fi; \
	fi; \
	if [ -z "$$upload_password" ]; then \
		echo "ERROR: upload password is empty."; \
		exit 1; \
	fi; \
	artifact_name=`sed -n 's/^filename:[[:space:]]*//p' "$(LATEST_FILE)" | head -n1 | tr -d '\r'`; \
		artifact_path="$(BUILD_DIR)/$$artifact_name"; \
		latest_path="$(LATEST_FILE)"; \
		artifact_url="$$upload_base/$(PUBLISH_PATH)/$$artifact_name"; \
		latest_url="$$upload_base/$(PUBLISH_PATH)/latest.txt"; \
		artifact_ok=0; \
		attempt=1; \
		while [ "$$attempt" -le "$(UPLOAD_RETRY_MAX)" ]; do \
			echo "publish $$artifact_name to server (attempt $$attempt/$(UPLOAD_RETRY_MAX))..."; \
			if command -v pv >/dev/null 2>&1; then \
				if pv -f "$$artifact_path" | curl $(CURL_UPLOAD_ARGS) --user "$$upload_username:$$upload_password" -T - "$$artifact_url"; then \
					artifact_ok=1; \
					break; \
				fi; \
			else \
				if curl $(CURL_UPLOAD_ARGS) --progress-bar --stderr - --user "$$upload_username:$$upload_password" -T "$$artifact_path" "$$artifact_url"; then \
					artifact_ok=1; \
					break; \
				fi; \
			fi; \
			if [ "$$attempt" -lt "$(UPLOAD_RETRY_MAX)" ]; then \
				echo "WARN: upload $$artifact_name failed, retrying in $(UPLOAD_RETRY_DELAY)s..."; \
				sleep "$(UPLOAD_RETRY_DELAY)"; \
			fi; \
			attempt=$$((attempt + 1)); \
		done; \
		if [ "$$artifact_ok" -ne 1 ]; then \
			echo "ERROR: upload $$artifact_name failed, skip latest.txt upload."; \
			exit 1; \
		fi; \
		latest_ok=0; \
		attempt=1; \
		while [ "$$attempt" -le "$(UPLOAD_RETRY_MAX)" ]; do \
			echo "publish latest.txt to server (attempt $$attempt/$(UPLOAD_RETRY_MAX))..."; \
			if command -v pv >/dev/null 2>&1; then \
				if pv -f "$$latest_path" | curl $(CURL_UPLOAD_ARGS) --user "$$upload_username:$$upload_password" -T - "$$latest_url"; then \
					latest_ok=1; \
					break; \
				fi; \
			else \
				if curl $(CURL_UPLOAD_ARGS) --progress-bar --stderr - --user "$$upload_username:$$upload_password" -T "$$latest_path" "$$latest_url"; then \
					latest_ok=1; \
					break; \
				fi; \
			fi; \
			if [ "$$attempt" -lt "$(UPLOAD_RETRY_MAX)" ]; then \
				echo "WARN: upload latest.txt failed, retrying in $(UPLOAD_RETRY_DELAY)s..."; \
				sleep "$(UPLOAD_RETRY_DELAY)"; \
			fi; \
			attempt=$$((attempt + 1)); \
		done; \
		if [ "$$latest_ok" -ne 1 ]; then \
			echo "ERROR: upload latest.txt failed."; \
			exit 1; \
		fi

publish:
	@upload_base="$(UPLOAD_BASE)"; \
	upload_username="$(UPLOAD_USERNAME)"; \
	upload_password="$(UPLOAD_PASSWORD)"; \
	if [ -z "$$upload_base" ]; then \
		if [ -t 0 ]; then \
			printf "upload base: "; \
			IFS= read -r upload_base; \
		else \
			echo "ERROR: UPLOAD_BASE is empty."; \
			exit 1; \
		fi; \
	fi; \
	if [ -z "$$upload_base" ]; then \
		echo "ERROR: upload base is empty."; \
		exit 1; \
	fi; \
	if [ -z "$$upload_username" ]; then \
		if [ -t 0 ]; then \
			printf "upload username: "; \
			IFS= read -r upload_username; \
		else \
			echo "ERROR: UPLOAD_USERNAME is empty."; \
			exit 1; \
		fi; \
	fi; \
	if [ -z "$$upload_username" ]; then \
		echo "ERROR: upload username is empty."; \
		exit 1; \
	fi; \
	if [ -z "$$upload_password" ]; then \
		if [ -t 0 ]; then \
			printf "upload password: "; \
			trap 'stty echo' EXIT INT TERM; \
			stty -echo; \
			IFS= read -r upload_password; \
			stty echo; \
			trap - EXIT INT TERM; \
			printf "\n"; \
		else \
			echo "ERROR: UPLOAD_PASSWORD is empty."; \
			exit 1; \
		fi; \
	fi; \
	if [ -z "$$upload_password" ]; then \
		echo "ERROR: upload password is empty."; \
		exit 1; \
	fi; \
	UPLOAD_BASE="$$upload_base" UPLOAD_USERNAME="$$upload_username" UPLOAD_PASSWORD="$$upload_password" $(MAKE) --no-print-directory clean && \
	UPLOAD_BASE="$$upload_base" UPLOAD_USERNAME="$$upload_username" UPLOAD_PASSWORD="$$upload_password" $(MAKE) --no-print-directory linux && \
	UPLOAD_BASE="$$upload_base" UPLOAD_USERNAME="$$upload_username" UPLOAD_PASSWORD="$$upload_password" $(MAKE) --no-print-directory pack && \
	UPLOAD_BASE="$$upload_base" UPLOAD_USERNAME="$$upload_username" UPLOAD_PASSWORD="$$upload_password" $(MAKE) --no-print-directory upload
