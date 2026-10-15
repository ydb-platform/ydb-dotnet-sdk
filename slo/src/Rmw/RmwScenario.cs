using System.Diagnostics;
using System.Runtime.ExceptionServices;

namespace Rmw;

public sealed record RmwOperation(string Id, int Counter, long Delta);

public sealed record RmwCounter(int Id, long Value, long Version);

public sealed record RmwSnapshot(IReadOnlyList<RmwOperation> Operations, IReadOnlyList<RmwCounter> Counters);

public interface IRmwTransaction
{
    Task<RmwOperation?> ReadOperation(string id);
    Task<RmwCounter> ReadCounter(int id);
    Task Write(RmwOperation operation, RmwCounter counter);
}

public interface IRmwStore
{
    Task ExecuteTransaction(Func<IRmwTransaction, Task> callback);
    Task<RmwSnapshot> ReadSnapshot();
}

public static class RmwScenario
{
    public static async Task Apply(IRmwTransaction transaction, RmwOperation operation)
    {
        var previous = await transaction.ReadOperation(operation.Id);
        if (previous != null)
        {
            if (previous != operation)
                throw new InvalidOperationException($"RMW operation payload mismatch: {operation.Id}");
            return;
        }

        var counter = await transaction.ReadCounter(operation.Counter);
        await transaction.Write(operation, counter with
        {
            Value = checked(counter.Value + operation.Delta),
            Version = checked(counter.Version + 1)
        });
    }

    // Stop prevents new logical operations; it never cancels a transaction with an uncertain outcome.
    public static async Task<int> Run(IRmwStore store, int writers, int counters,
        CancellationToken stop, int operationsPerWriter = int.MaxValue,
        Action<bool, TimeSpan>? record = null)
    {
        if (writers < 2 || counters < 1 || operationsPerWriter < 1)
            throw new ArgumentOutOfRangeException(nameof(writers));

        var expected = new List<RmwOperation>();
        ExceptionDispatchInfo? firstFailure = null;
        for (var sequence = 0; sequence < operationsPerWriter && !stop.IsCancellationRequested; sequence++)
        {
            // Every admitted writer settles before another round can update its hot counters.
            var round = Enumerable.Range(0, writers)
                .Select(writer => new RmwOperation($"{writer}:{sequence}", sequence % counters, writer + 1))
                .ToArray();
            expected.AddRange(round);
            await Task.WhenAll(round.Select(Write));
            firstFailure?.Throw();
        }

        Validate(expected, counters, await store.ReadSnapshot());
        return expected.Count;

        async Task Write(RmwOperation operation)
        {
            var started = Stopwatch.GetTimestamp();
            try
            {
                await store.ExecuteTransaction(transaction => Apply(transaction, operation));
                record?.Invoke(true, Stopwatch.GetElapsedTime(started));
            }
            catch (Exception error)
            {
                Interlocked.CompareExchange(ref firstFailure, ExceptionDispatchInfo.Capture(error), null);
                record?.Invoke(false, Stopwatch.GetElapsedTime(started));
            }
        }
    }

    public static void Validate(IReadOnlyList<RmwOperation> expected, int counters, RmwSnapshot actual)
    {
        var byId = expected.ToDictionary(x => x.Id);
        var seen = new HashSet<string>();
        foreach (var operation in actual.Operations)
        {
            if (!seen.Add(operation.Id) || !byId.TryGetValue(operation.Id, out var wanted) || wanted != operation)
                throw new InvalidOperationException($"RMW ledger mismatch: {operation.Id}");
        }

        if (seen.Count != byId.Count)
            throw new InvalidOperationException("RMW ledger is missing logical operations");

        var counterIds = new HashSet<int>();
        foreach (var counter in actual.Counters)
        {
            if (counter.Id < 0 || counter.Id >= counters || !counterIds.Add(counter.Id))
                throw new InvalidOperationException($"RMW unexpected counter: {counter.Id}");
            var entries = expected.Where(x => x.Counter == counter.Id).ToArray();
            if (counter.Value != entries.Sum(x => x.Delta) || counter.Version != entries.LongLength)
                throw new InvalidOperationException($"RMW counter value/version mismatch: {counter.Id}");
        }

        if (counterIds.Count != counters)
            throw new InvalidOperationException("RMW counters are missing");
    }
}
