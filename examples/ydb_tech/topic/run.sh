#!/usr/bin/env bash
set -euo pipefail
cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
dotnet run --project examples/ydb_tech/topic/Topic.csproj
