#!/usr/bin/env bash

set -eu

DIR="$1"
SLN_FILE="$2"

cd "$DIR"

jb cleanupcode "$SLN_FILE" --profile="Custom Cleanup"
