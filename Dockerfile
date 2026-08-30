FROM golang:1.26 AS build
ENV CGO_ENABLED 0
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
WORKDIR /go/src/github.com/fq-db/fq
COPY . .

RUN go mod download
RUN go build --ldflags "-w -s -extldflags -static \
    -X 'github.com/fq-db/fq/internal/version.Version=${VERSION}' \
    -X 'github.com/fq-db/fq/internal/version.Commit=${COMMIT}' \
    -X 'github.com/fq-db/fq/internal/version.Date=${BUILD_DATE}'" -o ./bin/fq ./cmd/fq/
RUN go build --ldflags "-w -s -extldflags -static \
    -X 'github.com/fq-db/fq/internal/version.Version=${VERSION}' \
    -X 'github.com/fq-db/fq/internal/version.Commit=${COMMIT}' \
    -X 'github.com/fq-db/fq/internal/version.Date=${BUILD_DATE}'" -o ./bin/fq-cli ./cmd/cli/

FROM alpine:latest
WORKDIR /app
COPY --from=build /go/src/github.com/fq-db/fq/bin/fq ./fq
COPY --from=build /go/src/github.com/fq-db/fq/bin/fq-cli ./fq-cli
COPY --from=build /go/src/github.com/fq-db/fq/config.yml ./config.yml
RUN mkdir -p /app/fq_data/wal
RUN chown nobody: /app -R

USER nobody
CMD ["/app/fq"]
