# CoordinationService SLO Workload

Chaos-oriented workload for YDB Coordination.

The workload creates a coordination node and a semaphore whose data contains a
monotonically increasing version. During `run` it starts:

- one writer that continuously updates the semaphore payload;
- several readers that poll `DescribeSemaphore(DataOnly)`;
- one watcher that follows `WatchSemaphore(DataOnly, WatchData)`.

Readers and the watcher keep their own last observed version. Seeing a smaller
version than before is treated as an SLO invariant failure and stops the
process. Transient request failures are logged and retried by recreating the
operation or watch.

## Usage

```bash
cd slo/src/CoordinationService

dotnet run -- create "Host=localhost;Port=2136;Database=/Root/testdb"

dotnet run -- run "Host=localhost;Port=2136;Database=/Root/testdb" \
  --read-rps 1000 \
  --write-rps 100 \
  --time 600
```

Run it together with `slo/playground`, whose `chaos` container stops,
restarts, and kills random `ydb-database-*` nodes.
