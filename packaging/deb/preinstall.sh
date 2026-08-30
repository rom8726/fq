#!/bin/sh
set -e

if ! getent group fq >/dev/null 2>&1; then
    groupadd --system fq
fi

if ! id -u fq >/dev/null 2>&1; then
    useradd --system \
        --gid fq \
        --home-dir /var/lib/fq \
        --no-create-home \
        --shell /usr/sbin/nologin \
        fq
fi
