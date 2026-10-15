using System.Collections.Concurrent;
using Moq;
using Rmw;
using Xunit;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Ado.RetryPolicy;
using Ydb.Sdk.Ado.Tests.Utils;

namespace Ydb.Sdk.Ado.Tests;

public class RmwScenarioTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);
    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    [Fact]
    public async Task HealthyContention_RetriesConflictingSnapshots_AndChecksFullLedger()
    {
        var store = new Store { ForceContention = true };
        Assert.Equal(32, await RmwScenario.Run(store, 4, 2, default, 8).WaitAsync(Timeout));
        Assert.True(store.Conflicts > 0);
        Assert.Equal(32, store.Snapshot.Operations.Count);
        Assert.True(store.CheckedAfterSettled);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CallbackRetry_ReusesId_AndAmbiguousPersistenceDoesNotIncrementTwice(bool persisted)
    {
        var store = new Store();
        var injected = 0;

        Task Inject(RmwOperation operation)
        {
            if (Interlocked.Increment(ref injected) == 1)
                throw new YdbException(persisted ? StatusCode.Undetermined : StatusCode.Aborted, "retry cause");
            return Task.CompletedTask;
        }

        if (persisted) store.AfterCommit = Inject;
        else store.BeforeCommit = Inject;
        Assert.Equal(2, await RmwScenario.Run(store, 2, 2, default, 1).WaitAsync(Timeout));
        Assert.Equal(2, store.Snapshot.Operations.Count);
        Assert.Equal(3, store.Snapshot.Counters.Sum(x => x.Value));
        Assert.Equal(2, store.Snapshot.Counters.Sum(x => x.Version));
        Assert.Contains(store.AttemptIds.GroupBy(x => x), group => group.Count() >= 2);
        Assert.Equal(2, store.AttemptIds.Distinct().Count());
    }

    [Theory]
    [InlineData("sum-preserving")]
    [InlineData("coherent-missing")]
    [InlineData("lost-value")]
    [InlineData("lost-version")]
    [InlineData("missing-op")]
    [InlineData("extra-op")]
    [InlineData("duplicate-op")]
    [InlineData("wrong-counter")]
    [InlineData("wrong-delta")]
    [InlineData("missing-counter")]
    [InlineData("extra-counter")]
    [InlineData("duplicate-counter")]
    public async Task IndependentInvariant_RejectsInvalidCompletedData(string fault)
    {
        var store = new Store();
        await RmwScenario.Run(store, 2, 2, default, 1).WaitAsync(Timeout);
        RmwOperation[] expected = [new("0:0", 0, 1), new("1:0", 0, 2)];
        var operations = store.Snapshot.Operations.ToList();
        var counters = store.Snapshot.Counters.ToList();
        switch (fault)
        {
            case "sum-preserving":
                counters[0] = counters[0] with { Value = counters[0].Value + 1 };
                counters[1] = counters[1] with { Value = counters[1].Value - 1 };
                break;
            case "coherent-missing":
                var missing = operations[0];
                operations.RemoveAt(0);
                var index = counters.FindIndex(counter => counter.Id == missing.Counter);
                counters[index] = counters[index] with
                {
                    Value = counters[index].Value - missing.Delta, Version = counters[index].Version - 1
                };
                break;
            case "lost-value": counters[0] = counters[0] with { Value = 1 }; break;
            case "lost-version": counters[0] = counters[0] with { Version = 1 }; break;
            case "missing-op": operations.RemoveAt(0); break;
            case "extra-op": operations.Add(new RmwOperation("extra", 0, 1)); break;
            case "duplicate-op": operations.Add(operations[0]); break;
            case "wrong-counter": operations[0] = operations[0] with { Counter = 1 }; break;
            case "wrong-delta": operations[0] = operations[0] with { Delta = 99 }; break;
            case "missing-counter": counters.RemoveAt(0); break;
            case "extra-counter": counters.Add(new RmwCounter(2, 0, 0)); break;
            case "duplicate-counter": counters.Add(counters[0]); break;
        }

        Assert.Throws<InvalidOperationException>(() => RmwScenario.Validate(expected, 2,
            new RmwSnapshot(operations, counters)));
    }

    [Fact]
    public async Task Deduplication_RejectsPreviouslyPersistedWrongPayload()
    {
        var store = new Store();
        var operation = new RmwOperation("fixed", 0, 1);
        await store.ExecuteTransaction(transaction => RmwScenario.Apply(transaction, operation));
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.ExecuteTransaction(transaction =>
            RmwScenario.Apply(transaction, operation with { Delta = 2 })));
        Assert.Single(store.Snapshot.Operations);
    }

    [Fact]
    public async Task PlannedStop_SettlesBothInflightTransactions_BeforeSnapshot()
    {
        using var stop = new CancellationTokenSource();
        var entered = Signal();
        var release = Signal();
        var arrivals = 0;
        var store = new Store
        {
            BeforeCommit = async _ =>
            {
                if (Interlocked.Increment(ref arrivals) == 2) entered.TrySetResult();
                await release.Task.WaitAsync(Timeout);
            }
        };
        var run = RmwScenario.Run(store, 2, 2, stop.Token);
        await entered.Task.WaitAsync(Timeout);
        stop.Cancel();
        Assert.False(run.IsCompleted);
        release.SetResult();
        Assert.Equal(2, await run.WaitAsync(Timeout));
        Assert.True(store.CheckedAfterSettled);
    }

    [Fact]
    public async Task FinalCause_IsOriginal_AndOtherWriterIsSettled()
    {
        var original = new YdbException(StatusCode.BadRequest, "original terminal transaction");
        var entered = Signal();
        var failed = Signal();
        var release = Signal();
        var arrivals = 0;
        var store = new Store
        {
            BeforeCommit = async operation =>
            {
                if (Interlocked.Increment(ref arrivals) == 2) entered.TrySetResult();
                await entered.Task.WaitAsync(Timeout);
                if (operation.Id == "0:0")
                {
                    failed.TrySetResult();
                    throw original;
                }

                await release.Task.WaitAsync(Timeout);
            }
        };
        var run = RmwScenario.Run(store, 2, 2, default, 1);
        await failed.Task.WaitAsync(Timeout);
        Assert.False(run.IsCompleted);
        release.SetResult();
        Assert.Same(original, await Assert.ThrowsAsync<YdbException>(() => run.WaitAsync(Timeout)));
        Assert.Equal(0, store.Active);
        Assert.False(store.CheckedAfterSettled);
    }

    [Fact]
    public async Task ExhaustedRetry_RemainsOriginalTerminalCause()
    {
        var original = new YdbException(StatusCode.Aborted, "retry exhausted");
        var store = new Store { BeforeCommit = _ => throw original };
        var error = await Assert.ThrowsAsync<YdbException>(() =>
            RmwScenario.Run(store, 2, 2, default, 1).WaitAsync(Timeout));
        Assert.Same(original, error);
        Assert.Empty(store.Snapshot.Operations);
    }

    // Fake storage is only the transaction boundary. The callback, runner and checker are shipped code;
    // retries execute the real SDK executor with zero jitter, not a parallel retry implementation.
    private sealed class Store : IRmwStore
    {
        private readonly object _lock = new();
        private readonly TaskCompletionSource _bothRead = Signal();
        private readonly YdbRetryPolicyExecutor _retry;
        private Dictionary<string, RmwOperation> _operations = new();

        private Dictionary<int, RmwCounter> _counters = new()
        {
            [0] = new RmwCounter(0, 0, 0),
            [1] = new RmwCounter(1, 0, 0)
        };

        private int _generation;
        private int _reads;
        public int Active;
        public int Conflicts;
        public bool ForceContention;
        public bool CheckedAfterSettled;
        public readonly List<string> AttemptIds = new();
        public Func<RmwOperation, Task>? BeforeCommit;
        public Func<RmwOperation, Task>? AfterCommit;

        public Store()
        {
            var random = new Mock<IRandom>();
            random.Setup(x => x.Next(It.IsAny<int>())).Returns(0);
            _retry = new YdbRetryPolicyExecutor(new YdbRetryPolicy(
                new YdbRetryPolicyConfig { EnableRetryIdempotence = true }, random.Object));
        }

        public RmwSnapshot Snapshot
        {
            get
            {
                lock (_lock) return new RmwSnapshot(_operations.Values.ToArray(), _counters.Values.ToArray());
            }
        }

        public async Task ExecuteTransaction(Func<IRmwTransaction, Task> callback)
        {
            Interlocked.Increment(ref Active);
            try
            {
                await _retry.ExecuteAsync(async _ =>
                {
                    Transaction transaction;
                    int generation;
                    lock (_lock)
                    {
                        generation = _generation;
                        transaction = new Transaction(this, new Dictionary<string, RmwOperation>(_operations),
                            new Dictionary<int, RmwCounter>(_counters));
                    }

                    await callback(transaction);
                    if (transaction.Written != null && BeforeCommit != null)
                        await BeforeCommit(transaction.Written);
                    lock (_lock)
                    {
                        if (generation != _generation)
                        {
                            Conflicts++;
                            throw new YdbException(StatusCode.Aborted, "snapshot conflict");
                        }

                        _operations = transaction.Operations;
                        _counters = transaction.Counters;
                        _generation++;
                    }

                    if (transaction.Written != null && AfterCommit != null)
                        await AfterCommit(transaction.Written);
                });
            }
            finally
            {
                Interlocked.Decrement(ref Active);
            }
        }

        public Task<RmwSnapshot> ReadSnapshot()
        {
            Assert.Equal(0, Volatile.Read(ref Active));
            CheckedAfterSettled = true;
            return Task.FromResult(Snapshot);
        }

        private sealed class Transaction(
            Store store,
            Dictionary<string, RmwOperation> operations,
            Dictionary<int, RmwCounter> counters) : IRmwTransaction
        {
            public Dictionary<string, RmwOperation> Operations => operations;
            public Dictionary<int, RmwCounter> Counters => counters;
            public RmwOperation? Written;

            public Task<RmwOperation?> ReadOperation(string id)
            {
                lock (store._lock) store.AttemptIds.Add(id);
                return Task.FromResult(operations.GetValueOrDefault(id));
            }

            public async Task<RmwCounter> ReadCounter(int id)
            {
                if (store.ForceContention)
                {
                    var arrival = Interlocked.Increment(ref store._reads);
                    if (arrival <= 2)
                    {
                        if (arrival == 2) store._bothRead.TrySetResult();
                        await store._bothRead.Task.WaitAsync(Timeout);
                    }
                }

                return counters[id];
            }

            public Task Write(RmwOperation operation, RmwCounter counter)
            {
                operations.Add(operation.Id, operation);
                counters[counter.Id] = counter;
                Written = operation;
                return Task.CompletedTask;
            }
        }
    }
}

public class RmwNativeTests
{
    [Fact]
    public async Task SerializableAdoTransactions_ContendingWritersAndIndependentNamespaces()
    {
        await using var unrelated = new YdbConnection(TestUtils.ConnectionString);
        await unrelated.OpenAsync();
        // Disposing this datasource must not clear the pool used by parallel SDK tests.
        var settings = new YdbConnectionStringBuilder(TestUtils.ConnectionString)
            { ClientInfo = $"slo-rmw-native-{Guid.NewGuid():N}/1.0.0" };
        await using (var client = new YdbDataSourceBuilder(settings)
                         { RetryPolicy = YdbRetryPolicy.IdempotenceDefault }.Build())
        {
            var currentAttempts = new ConcurrentBag<int>();
            var peerAttempts = new ConcurrentBag<int>();
            using var current = new AdoRmwStore(client, 30, 30, currentAttempts.Add);
            using var peer = new AdoRmwStore(client, 30, 30, peerAttempts.Add);
            var contended = new ContendedStore(current);
            Assert.NotEqual(current.Namespace, peer.Namespace);
            try
            {
                await Task.WhenAll(current.Create(2), peer.Create(2));
                var results = await Task.WhenAll(
                    RmwScenario.Run(contended, 4, 2, default, 6),
                    RmwScenario.Run(peer, 4, 2, default, 6)).WaitAsync(TimeSpan.FromSeconds(90));
                Assert.Equal(new[] { 24, 24 }, results);
                Assert.Contains(contended.Attempts.Values, attempts => attempts > 1);
                Assert.Equal(24, currentAttempts.Count);
                Assert.Equal(24, peerAttempts.Count);
                Assert.True(currentAttempts.Sum() >= contended.Attempts.Values.Sum());
                Assert.True(currentAttempts.Sum() > 24);
            }
            finally
            {
                await Task.WhenAll(current.Drop(), peer.Drop());
            }
        }

        await using var command = new YdbCommand("SELECT 1;", unrelated);
        Assert.Equal(1, await command.ExecuteScalarAsync());
    }

    // Hold the first four actual counter reads before allowing writes. At least one transaction
    // must conflict; no sleep, failure injection or product retry-policy override is involved.
    private sealed class ContendedStore(IRmwStore store) : IRmwStore
    {
        private readonly TaskCompletionSource _read = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _arrivals;
        public readonly ConcurrentDictionary<string, int> Attempts = new();
        public Task<RmwSnapshot> ReadSnapshot() => store.ReadSnapshot();

        public Task ExecuteTransaction(Func<IRmwTransaction, Task> callback) =>
            store.ExecuteTransaction(transaction => callback(new Transaction(this, transaction)));

        private sealed class Transaction(ContendedStore owner, IRmwTransaction inner) : IRmwTransaction
        {
            public Task<RmwOperation?> ReadOperation(string id)
            {
                owner.Attempts.AddOrUpdate(id, 1, (_, attempts) => attempts + 1);
                return inner.ReadOperation(id);
            }

            public async Task<RmwCounter> ReadCounter(int id)
            {
                var counter = await inner.ReadCounter(id);
                var arrival = Interlocked.Increment(ref owner._arrivals);
                if (arrival <= 4)
                {
                    if (arrival == 4) owner._read.TrySetResult();
                    await owner._read.Task.WaitAsync(TimeSpan.FromSeconds(30));
                }

                return counter;
            }

            public Task Write(RmwOperation operation, RmwCounter counter) => inner.Write(operation, counter);
        }
    }
}
