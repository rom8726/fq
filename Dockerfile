FROM golang:1.22 AS build
ENV CGO_ENABLED 0
WORKDIR /go/src/github.com/rom8726/fq
COPY . .

RUN go mod download
RUN go build --ldflags "-w -s -extldflags -static -X 'main.Commit=$(git rev-parse HEAD)'" -o ./bin/fq ./cmd/fq/

FROM alpine:latest
WORKDIR /app
COPY --from=build /go/src/github.com/rom8726/fq/bin/fq ./fq
COPY --from=build /go/src/github.com/rom8726/fq/config.yml ./config.yml
RUN mkdir -p /app/data/wal
RUN chown nobody: /app -R

USER nobody
CMD ["/app/fq"]
