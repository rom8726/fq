WAL_ROOT = $(PWD)/internal/database/storage/wal
BIN_DIR = $(PWD)/bin

.PHONY: build
build: build-fq build-cli build-bench

.PHONY: build-fq
build-fq:
	@echo "-> Building fq server binary..."
	@mkdir -p $(BIN_DIR)
	@go build -o $(BIN_DIR)/fq ./cmd/fq
	@echo "-> Binary built: $(BIN_DIR)/fq"

.PHONY: build-cli
build-cli:
	@echo "-> Building fq CLI client binary..."
	@mkdir -p $(BIN_DIR)
	@go build -o $(BIN_DIR)/fq-cli ./cmd/cli
	@echo "-> Binary built: $(BIN_DIR)/fq-cli"

.PHONY: build-bench
build-bench:
	@echo "-> Building fq benchmark binary..."
	@mkdir -p $(BIN_DIR)
	@go build -o $(BIN_DIR)/fq-bench ./cmd/bench
	@echo "-> Binary built: $(BIN_DIR)/fq-bench"

.PHONY: run-server
run-server:
	@echo "-> Running fq server (master)..."
	@mkdir -p ./fq_data/wal
	@go run ./cmd/fq

.PHONY: run-slave
run-slave:
	@echo "-> Running fq server (slave replica)..."
	@mkdir -p ./fq_data-slave/wal
	@go run ./cmd/fq config-slave.yml

.PHONY: run-cli
run-cli:
	@echo "-> Running fq CLI client..."
	@go run ./cmd/cli -address :1945

.PHONY: run-cli-slave
run-cli-slave:
	@echo "-> Running fq CLI client for slave..."
	@go run ./cmd/cli -address :1947

.PHONY: run-bench
run-bench:
	@echo "-> Running fq benchmark..."
	@go run ./cmd/bench -address :1945

.PHONY: bench-smoke
bench-smoke:
	@echo "-> Running fq benchmark smoke profile..."
	@mkdir -p ./benchmarks/results
	@go run ./cmd/bench -profile ./benchmarks/profiles/smoke.yml

.PHONY: bench-release
bench-release:
	@echo "-> Running fq benchmark release profile (uniform counter)..."
	@mkdir -p ./benchmarks/results
	@go run ./cmd/bench -profile ./benchmarks/profiles/release-uniform-counter.yml

.PHONY: bench-release-all
bench-release-all:
	@echo "-> Running fq benchmark release profiles..."
	@mkdir -p ./benchmarks/results
	@go run ./cmd/bench -profile ./benchmarks/profiles/release-hot-counter.yml
	@go run ./cmd/bench -profile ./benchmarks/profiles/release-uniform-counter.yml
	@go run ./cmd/bench -profile ./benchmarks/profiles/release-fw.yml
	@go run ./cmd/bench -profile ./benchmarks/profiles/release-sw.yml
	@go run ./cmd/bench -profile ./benchmarks/profiles/release-tb.yml

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
