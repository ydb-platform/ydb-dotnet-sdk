#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
dotnet run --project ydb_tech/topic/Topic.csproj
