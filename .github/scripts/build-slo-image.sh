#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat >&2 << 'ENDUSAGE'
Usage:
  build-slo-image.sh \
    --context <path> \
    --tag <docker-tag> \
    --workload <workload-name>

Options:
  --context     Docker build context directory (e.g. $GITHUB_WORKSPACE/current).
  --tag         Docker image tag to build (e.g. ydb-app-current).
  --workload    Workload name (e.g. AdoNet, Dapper, EF, Linq2db.Slo, TopicService).
ENDUSAGE
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

context_dir=""
tag=""
workload=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --context)
      context_dir="${2:-}"
      shift 2
      ;;
    --tag)
      tag="${2:-}"
      shift 2
      ;;
    --workload)
      workload="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown argument: $1 (use --help)"
      ;;
  esac
done

if [[ -z "$context_dir" || -z "$tag" || -z "$workload" ]]; then
  usage
  exit 2
fi

[[ -d "$context_dir" ]] || die "--context does not exist: $context_dir"
context_dir="$(cd "$context_dir" && pwd)"

dockerfile="slo/src/${workload}/Dockerfile"
[[ -f "$context_dir/$dockerfile" ]] || die "Dockerfile not found: $context_dir/$dockerfile"

echo "Building SLO image..."
echo "  TAG:        $tag"
echo "  WORKLOAD:   $workload"
echo "  DOCKERFILE: $dockerfile"

(
  cd "$context_dir"
  docker build -t "$tag" \
    -f "$dockerfile" .
)
