# SLO Testing for YDB .NET SDK

**SLO (Service Level Objectives)** tests verify SDK reliability under adverse conditions:
node failures, tablet restarts, and network partitions — the kind of events that happen routinely
in a large distributed database cluster.

## How it runs

SLO is executed entirely from GitHub Actions via
[`ydb-platform/ydb-slo-action`](https://github.com/ydb-platform/ydb-slo-action), which deploys
the YDB cluster, runs Prometheus, injects chaos and (for table workloads) compares the PR build
against `main`.

| Workflow | Workloads | Trigger |
|---|---|---|
| [`.github/workflows/slo.yml`](../.github/workflows/slo.yml) | `AdoNet`, `Dapper`, `EF`, `Linq2db` | PR with `SLO` label |
| [`.github/workflows/slo-topic.yaml`](../.github/workflows/slo-topic.yaml) | `TopicService` | push to `main`, every PR, hourly cron |

## Structure

```
slo/src/
├── Internal/        Shared CLI + metrics (OTLP) + workload harness
├── AdoNet/          ADO.NET driver workload
├── Dapper/          Dapper workload
├── EF/              EF Core workload
├── Linq2db/         linq2db workload
└── TopicService/    Topic API workload
```

Each workload project is a self-contained dotnet app whose `Dockerfile` produces the image
consumed by the action (`workload_current_image`).

## Workload CLI

See [`src/README.md`](./src/README.md) for the `run` command flags.
