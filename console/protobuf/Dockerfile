# syntax=docker/dockerfile:1

FROM golang:1.26.8-alpine@sha256:34efdd6036c92e155c8b0162a5da7626586b612ea636590035602c970eece564

ENV USER user
ENV HOME /home/${USER}

RUN apk add --update sudo
RUN adduser -D $USER \
        && echo "$USER ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/$USER \
        && chmod 0440 /etc/sudoers.d/$USER
USER $USER

WORKDIR /go/src/service

RUN sudo apk add build-base
RUN sudo apk add protobuf-dev
RUN sudo apk add protoc

RUN go install github.com/golang/protobuf/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
