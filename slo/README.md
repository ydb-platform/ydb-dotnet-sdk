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
| [`.github/workflows/slo.yml`](../.github/workflows/slo.yml) | `AdoNet`, `Dapper`, `EF`, `Linq2db`, `Rmw` | PR with `SLO` label |
| [`.github/workflows/slo-topic.yaml`](../.github/workflows/slo-topic.yaml) | `TopicService` | push to `main`, every PR, hourly cron |

## Structure

```
slo/src/
├── Internal/        Shared CLI + metrics (OTLP) + workload harness
├── AdoNet/          ADO.NET driver workload
├── Dapper/          Dapper workload
├── EF/              EF Core workload
├── Linq2db/         linq2db workload
├── Rmw/             Separate concurrent transactional ADO.NET workload
└── TopicService/    Topic API workload
```

Each workload project is a self-contained dotnet app whose `Dockerfile` produces the image
consumed by the action (`workload_current_image`).

## Workload CLI

See [`src/README.md`](./src/README.md) for the `run` command flags.

## Concurrent transactional RMW

`Rmw` is a separate executable, not a mode of the basic KV adapters. Only the SDK's
ADO.NET `YdbDataSource.ExecuteInTransactionAsync` adapter is implemented. Dapper,
EF Core and Linq2db RMW adapters are not implemented or implied by their KV coverage.

```sh
dotnet run --project slo/src/Rmw/Rmw.csproj -- run \
  'Host=localhost;Port=2136;Database=/local' --time 60 \
  --otlp-endpoint http://localhost:9090/api/v1/otlp/v1/metrics
```

Four writers contend on two counters without rate limiting. Each round waits for
all four admitted operations before another round can update the shared counters. The shared CLI accepts
the ordinary KV flags, but `--read-rps`, `--write-rps` and `--initial-data-count` do
not configure this scenario. `--time` (or `WORKLOAD_DURATION`), the read/write
command timeouts, OTLP endpoint and report period apply. The existing SDK
idempotent retry policy and its retry limits/backoff are retained.

Each process generates one private table namespace for setup, writes, checking and
cleanup. A logical ID is allocated before entering the retry callback. One
serializable RW transaction checks that ID's ledger entry, reads and increments
the counter value/version and inserts the ID/counter/delta entry. An already
persisted ID with the same payload is a no-op, including a retry after an
ambiguous commit; a different payload is terminal.

Duration expiry, SIGTERM and SIGINT stop new operations and let in-flight SDK
transactions settle. A separate serializable transaction then reads the entire
ledger and both counters. The checker compares the exact expected ID set and
payloads, rejects duplicates or unexpected/missing counters, and checks each
counter's value and version against the independently retained expected deltas
and operation count.
A final transaction failure preserves its original exception and fails the
process after other writers settle. Cleanup cannot replace that cause.

The ordinary SLO workflow builds this project as its own matrix entry. A baseline
image is built only when that baseline checkout already contains its own RMW
project; otherwise this new scenario runs current-only. Neither workload nor SDK
sources are copied between checkouts. Generated tables are dropped only by their
owning process. Existing KV and Topic executables are unchanged.

Legacy `sdk_operations_total` counts logical transactions. Technical attempts,
including acquisition and commit retries, come from the SDK's real
`ydb.client.retry.attempts` measurement and are exposed as
`sdk_retry_attempts_total`. Successful whole-operation latency uses the existing
client-side HDR p50/p95/p99 gauge families.

```sh
# Deterministic callback/runner/checker tests, with the actual SDK retry executor:
dotnet test src/Ydb.Sdk/test/Ydb.Sdk.Ado.Tests/ \
  --filter FullyQualifiedName~RmwScenarioTests

# Real ADO.NET transactions, forced contention via a test-only read barrier,
# recovered SDK retries and two simultaneous isolated lifecycle namespaces:
YDB_TEST_DB='Host=localhost;Port=2136;Database=/local' \
  dotnet test src/Ydb.Sdk/test/Ydb.Sdk.Ado.Tests/ \
  --filter FullyQualifiedName~RmwNativeTests
```

The native test requires YDB. Ambiguous persistence and data-negative unit tests
use a transaction-boundary fixture with the shipped callback/checker and actual
SDK retry executor; they are not evidence of a dropped real commit response or
of fault-injected behavior in another adapter.

The current SDK can recover a protocol `UNDETERMINED` commit response using the
same logical ID. A native transport-response-loss probe instead exposed an
existing SDK limitation: commit metrics can throw `Connection is closed` after
the session is deactivated, masking the transport error before the retry
executor sees it. This workload does not change or bypass that SDK behavior;
such a final failure still exits nonzero.
