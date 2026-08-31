WAL_ROOT = $(PWD)/internal/database/storage/wal
BIN_DIR = $(PWD)/bin

VERSION_PKG = github.com/fq-db/fq/internal/version
GIT_VERSION := $(shell git describe --tags --always --dirty 2>/dev/null | sed 's/^v//')
GIT_COMMIT := $(shell git rev-parse HEAD 2>/dev/null)
VERSION ?= $(if $(GIT_VERSION),$(GIT_VERSION),dev)
COMMIT ?= $(if $(GIT_COMMIT),$(GIT_COMMIT),unknown)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
GO_LDFLAGS = -X $(VERSION_PKG).Version=$(VERSION) -X $(VERSION_PKG).Commit=$(COMMIT) -X $(VERSION_PKG).Date=$(BUILD_DATE)

# Fixed tokens for local development only. Never reuse these anywhere else.
FQ_ADMIN_TOKEN ?= dev-admin-3f9a21c7b45e
FQ_RW_TOKEN ?= dev-rw-8c1d64e0a7b2
FQ_RO_TOKEN ?= dev-ro-5b7e03f9c8d1
FQ_SLAVE_ADMIN_TOKEN ?= dev-slave-admin-42a9c1
FQ_SLAVE_RO_TOKEN ?= dev-slave-ro-7e30b8d5
FQ_REPLICATION_TOKEN ?= dev-replication-9d24f6a1c3

FQ_MASTER_ENV = FQ_ADMIN_TOKEN=$(FQ_ADMIN_TOKEN) FQ_RW_TOKEN=$(FQ_RW_TOKEN) FQ_RO_TOKEN=$(FQ_RO_TOKEN) FQ_REPLICATION_TOKEN=$(FQ_REPLICATION_TOKEN)
FQ_SLAVE_ENV = FQ_SLAVE_ADMIN_TOKEN=$(FQ_SLAVE_ADMIN_TOKEN) FQ_SLAVE_RO_TOKEN=$(FQ_SLAVE_RO_TOKEN) FQ_REPLICATION_TOKEN=$(FQ_REPLICATION_TOKEN)

.PHONY: build
build: build-fq build-cli build-bench build-stress build-results

.PHONY: build-fq
build-fq:
	@echo "-> Building fq server binary..."
	@mkdir -p $(BIN_DIR)
	@go build -ldflags "$(GO_LDFLAGS)" -o $(BIN_DIR)/fq ./cmd/fq
	@echo "-> Binary built: $(BIN_DIR)/fq"

.PHONY: build-cli
build-cli:
	@echo "-> Building fq CLI client binary..."
	@mkdir -p $(BIN_DIR)
	@go build -ldflags "$(GO_LDFLAGS)" -o $(BIN_DIR)/fq-cli ./cmd/cli
	@echo "-> Binary built: $(BIN_DIR)/fq-cli"

.PHONY: build-bench
build-bench:
	@echo "-> Building fq benchmark binary..."
	@mkdir -p $(BIN_DIR)
	@go build -ldflags "$(GO_LDFLAGS)" -o $(BIN_DIR)/fq-bench ./cmd/bench
	@echo "-> Binary built: $(BIN_DIR)/fq-bench"

.PHONY: build-stress
build-stress:
	@echo "-> Building fq stress binary..."
	@mkdir -p $(BIN_DIR)
	@go build -ldflags "$(GO_LDFLAGS)" -o $(BIN_DIR)/fq-stress ./cmd/stress
	@echo "-> Binary built: $(BIN_DIR)/fq-stress"

.PHONY: build-results
build-results:
	@echo "-> Building fq results capture binary..."
	@mkdir -p $(BIN_DIR)
	@go build -ldflags "$(GO_LDFLAGS)" -o $(BIN_DIR)/fq-results ./cmd/results
	@echo "-> Binary built: $(BIN_DIR)/fq-results"

.PHONY: certs
certs:
	@echo "-> Generating development TLS certificates..."
	@./scripts/gen-certs.sh

.PHONY: certs-force
certs-force:
	@echo "-> Regenerating development TLS certificates..."
	@CERT_FORCE=1 ./scripts/gen-certs.sh

.PHONY: certs-clean
certs-clean:
	@echo "-> Removing development TLS certificates..."
	@rm -rf ./certs

.PHONY: run-server
run-server:
	@echo "-> Running fq server (master)..."
	@mkdir -p ./fq_data/wal
	@$(FQ_MASTER_ENV) go run ./cmd/fq -c ./config.yml

.PHONY: run-interactive
run-interactive:
	@echo "-> Running fq server with interactive TUI..."
	@mkdir -p ./fq_data/wal
	@$(FQ_MASTER_ENV) go run ./cmd/fq -i -c ./config.yml

.PHONY: run-slave
run-slave:
	@echo "-> Running fq server (slave replica)..."
	@mkdir -p ./fq_data-slave/wal
	@$(FQ_SLAVE_ENV) go run ./cmd/fq -c ./config-slave.yml

.PHONY: run-cli
run-cli:
	@echo "-> Running fq CLI client..."
	@go run ./cmd/cli -address :1945 -token $(FQ_ADMIN_TOKEN)

.PHONY: run-cli-slave
run-cli-slave:
	@echo "-> Running fq CLI client for slave..."
	@go run ./cmd/cli -address :1947 -token $(FQ_SLAVE_ADMIN_TOKEN)

.PHONY: run-cli-tls
run-cli-tls:
	@echo "-> Running fq TLS CLI client..."
	@go run ./cmd/cli -address localhost:1945 -token "$(FQ_ADMIN_TOKEN)" -tls_ca ./certs/ca.crt -tls_cert ./certs/client.crt -tls_key ./certs/client.key -tls_server_name localhost

.PHONY: run-cli-slave-tls
run-cli-slave-tls:
	@echo "-> Running fq TLS CLI client for slave..."
	@go run ./cmd/cli -address localhost:1947 -token "$(FQ_SLAVE_ADMIN_TOKEN)" -tls_ca ./certs/ca.crt -tls_cert ./certs/client.crt -tls_key ./certs/client.key -tls_server_name localhost

.PHONY: run-bench
run-bench:
	@echo "-> Running fq benchmark..."
	@go run ./cmd/bench -address :1945 -token $(FQ_ADMIN_TOKEN)

.PHONY: docker-run-interactive
docker-run-interactive:
	@echo "-> Running fq Docker image with interactive TUI..."
	@docker run --rm -it --entrypoint /var/lib/fq/fq \
		-p 1945:1945 \
		-p 1946:1946 \
		-p 2112:2112 \
		-e FQ_ADMIN_TOKEN=$(FQ_ADMIN_TOKEN) \
		-e FQ_RW_TOKEN=$(FQ_RW_TOKEN) \
		-e FQ_RO_TOKEN=$(FQ_RO_TOKEN) \
		-e FQ_REPLICATION_TOKEN=$(FQ_REPLICATION_TOKEN) \
		ghcr.io/fq-db/fq:latest -i

.PHONY: bench-smoke
bench-smoke:
	@echo "-> Running fq benchmark smoke profile..."
	@mkdir -p ./benchmarks/results
	@go run ./cmd/bench -profile ./benchmarks/profiles/smoke.yml -token $(FQ_ADMIN_TOKEN)

.PHONY: bench-release
bench-release:
	@echo "-> Running fq benchmark release profile (uniform counter)..."
	@mkdir -p ./benchmarks/results
	@go run ./cmd/bench -profile ./benchmarks/profiles/release-uniform-counter.yml -token $(FQ_ADMIN_TOKEN)

.PHONY: bench-release-all
bench-release-all:
	@echo "-> Running fq benchmark release profiles..."
	@mkdir -p ./benchmarks/results
	@go run ./cmd/bench -profile ./benchmarks/profiles/release-hot-counter.yml -token $(FQ_ADMIN_TOKEN)
	@go run ./cmd/bench -profile ./benchmarks/profiles/release-uniform-counter.yml -token $(FQ_ADMIN_TOKEN)
	@go run ./cmd/bench -profile ./benchmarks/profiles/release-fw.yml -token $(FQ_ADMIN_TOKEN)
	@go run ./cmd/bench -profile ./benchmarks/profiles/release-sw-uniform.yml -token $(FQ_ADMIN_TOKEN)
	@go run ./cmd/bench -profile ./benchmarks/profiles/release-sw-zipfian.yml -token $(FQ_ADMIN_TOKEN)
	@go run ./cmd/bench -profile ./benchmarks/profiles/release-tb.yml -token $(FQ_ADMIN_TOKEN)

.PHONY: stress-smoke
stress-smoke:
	@echo "-> Running fq stress restart smoke scenario..."
	@go run ./cmd/stress -scenario restart-smoke -duration 30s

.PHONY: stress-crash-loop
stress-crash-loop:
	@echo "-> Running fq stress crash-loop scenario..."
	@mkdir -p ./benchmarks/results
	@go run ./cmd/stress -scenario crash-loop -duration 30s -workers 4 -keys 100 -kill_interval 2s -seed 42 -report_file ./benchmarks/results/stress-crash-loop.json

.PHONY: stress-dump-recovery
stress-dump-recovery:
	@echo "-> Running fq stress dump recovery scenario..."
	@mkdir -p ./benchmarks/results
	@go run ./cmd/stress -scenario dump-recovery -duration 30s -workers 4 -keys 100 -kill_interval 2s -dump_interval 250ms -seed 42 -report_file ./benchmarks/results/stress-dump-recovery.json

.PHONY: stress-replication
stress-replication:
	@echo "-> Running fq stress replication scenario..."
	@mkdir -p ./benchmarks/results
	@go run ./cmd/stress -scenario replication-stress -duration 30s -workers 4 -keys 100 -kill_interval 2s -sync_interval 100ms -seed 42 -report_file ./benchmarks/results/stress-replication.json

.PHONY: results-plan
results-plan:
	@echo "-> Capturing fq release results metadata and command manifest..."
	@go run ./cmd/results -mode release

.PHONY: results-smoke
results-smoke:
	@echo "-> Running quick fq results smoke capture..."
	@go run ./cmd/results -mode smoke -run -benchmarks=false

.PHONY: lint
lint:
	golangci-lint -v run

.PHONY: test
test:
	go test -v ./...

.PHONY: test-race
test-race:
	go test -race -v ./...

.PHONY: proto.image.build
proto.image.build:
	@echo "-> build proto image"
	@docker build -f ./console/protobuf/Dockerfile ./console/protobuf -t fq_console_proto

.PHONY: proto.wal.build
proto.wal.build: proto.image.build
	@echo "-> Build WAL proto files"
	@docker run -v $(WAL_ROOT)/:/go/src/service/proto:rw --name fq_console_proto --rm -it fq_console_proto \
		sh -c "protoc -I /go/src/service/proto --go_out=/go/src/service/proto --go-grpc_out=/go/src/service/proto /go/src/service/proto/*.proto"
	@mv $(WAL_ROOT)/wal/log_data.pb.go $(WAL_ROOT)/log_data.pb.go
	@rm -R $(WAL_ROOT)/wal
