# Emulates a UW-Madison CS lab machine (Ubuntu 22.04, x86-64)
# Used for building and testing MiniSpark locally on macOS.
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    gcc \
    make \
    python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
