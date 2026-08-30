#!/bin/sh
set -e

if command -v systemctl >/dev/null 2>&1; then
    systemctl stop fq.service || true
    systemctl disable fq.service || true
fi
