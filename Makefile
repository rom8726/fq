WAL_ROOT = $(PWD)/internal/database/storage/wal
BIN_DIR = $(PWD)/bin

.PHONY: build
build: build-fq build-cli

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

.PHONY: run-server
run-server:
	@echo "-> Running fq server (master)..."
	@go run ./cmd/fq

.PHONY: run-slave
run-slave:
	@echo "-> Running fq server (slave replica)..."
	@go run ./cmd/fq config-slave.yml

.PHONY: run-cli
run-cli:
	@echo "-> Running fq CLI client..."
	@go run ./cmd/cli -address :1945

.PHONY: lint
lint:
	golangci-lint -v run

.PHONY: test
test:
	go test -v ./...

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
