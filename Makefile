WAL_ROOT = $(PWD)/internal/database/storage/wal

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
