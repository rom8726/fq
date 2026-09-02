FROM golang:1.26.8@sha256:5b88920df10d59b4d289ac74bd99eb6839df57ce5f558e740ebe0053e4235bd4 AS build
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

FROM alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce
WORKDIR /var/lib/fq
RUN mkdir -p /var/lib/fq/data/wal /var/lib/fq/certs /etc/fq
RUN chown nobody: /var/lib/fq -R
COPY --from=build /go/src/github.com/fq-db/fq/bin/fq ./fq
COPY --from=build /go/src/github.com/fq-db/fq/bin/fq-cli ./fq-cli
COPY --from=build /go/src/github.com/fq-db/fq/config.yml /etc/fq/config.yml

USER nobody
ENTRYPOINT ["/var/lib/fq/fq"]
