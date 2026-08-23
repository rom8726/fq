FROM golang:1.25 AS build
ENV CGO_ENABLED 0
ARG COMMIT=dev
WORKDIR /go/src/github.com/fq-db/fq
COPY . .

RUN go mod download
RUN go build --ldflags "-w -s -extldflags -static -X 'main.Commit=${COMMIT}'" -o ./bin/fq ./cmd/fq/
RUN go build --ldflags "-w -s -extldflags -static -X 'main.Commit=${COMMIT}'" -o ./bin/fq-cli ./cmd/cli/

FROM alpine:latest
WORKDIR /app
COPY --from=build /go/src/github.com/fq-db/fq/bin/fq ./fq
COPY --from=build /go/src/github.com/fq-db/fq/bin/fq-cli ./fq-cli
COPY --from=build /go/src/github.com/fq-db/fq/config.yml ./config.yml
RUN mkdir -p /app/data/wal
RUN chown nobody: /app -R

USER nobody
CMD ["/app/fq"]
