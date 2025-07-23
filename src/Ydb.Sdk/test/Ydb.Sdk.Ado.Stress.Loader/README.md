# YDB ADO.NET Stress Test Tank

Variable load generator for testing YDB ADO.NET driver performance with step-like pattern.

## Description Cycle Mode

The stress test "tank" generates cyclical load using a step-like pattern:

```
Peak RPS (1000) ─┐
                 │ Peak Duration
                 └─ Medium RPS (100) ─┐
                                      │ Medium Duration
                                      └─ Min RPS (1-2) ─┐
                                                        │ Min Duration
                                                        └─ Medium RPS (100) ─┐
                                                                             │ Medium Duration
                                                                             └─ (cycle repeats)
```

## Description Load Mode

```
Loading With PoolingSourceSession

sleep 10s

Loading Without PoolingSourceSession
```
