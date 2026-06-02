using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;
using Xunit;

namespace Ydb.Sdk.Ado.Tests;

/// <summary>
/// Regression tests for https://github.com/ydb-platform/ydb-dotnet-sdk/issues/647
///
/// The SDK exposes synchronous APIs (e.g. <see cref="YdbConnection.Open"/>) that are implemented
/// as sync-over-async (<c>OpenAsync().GetAwaiter().GetResult()</c>). If any <c>await</c> inside the
/// async implementation does not call <c>ConfigureAwait(false)</c>, the continuation is posted back
/// to the captured <see cref="SynchronizationContext"/>. When a caller blocks that single-threaded
/// context (the classic WPF/WinForms UI thread scenario), the continuation can never run and the
/// thread deadlocks.
///
/// Each test runs a blocking SDK call on a dedicated thread that owns a single-threaded
/// <see cref="SynchronizationContext"/> and fails if the call does not finish in time.
/// </summary>
public class ConfigureAwaitDeadlockTests : TestBase
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    [Fact]
    public void Open_OnSingleThreadedSynchronizationContext_DoesNotDeadlock()
    {
        AssertNoDeadlock(() =>
        {
            using var connection = CreateConnection();
            connection.Open();
        });
    }

    [Fact]
    public void ExecuteScalar_OnSingleThreadedSynchronizationContext_DoesNotDeadlock()
    {
        AssertNoDeadlock(() =>
        {
            using var connection = CreateConnection();
            connection.Open();

            using var command = connection.CreateCommand();
            command.CommandText = "SELECT 1;";

            Assert.Equal(1, command.ExecuteScalar());
        });
    }

    private static void AssertNoDeadlock(Action blockingCall)
    {
        using var context = new SingleThreadSynchronizationContext();

        var completed = context.TryRun(blockingCall, Timeout, out var error);

        Assert.True(completed,
            "Detected a deadlock: a synchronous SDK call did not complete within the timeout while running on a " +
            "single-threaded SynchronizationContext. This usually means an 'await' is missing 'ConfigureAwait(false)'.");

        if (error != null)
        {
            ExceptionDispatchInfo.Capture(error).Throw();
        }
    }

    /// <summary>
    /// A minimal single-threaded <see cref="SynchronizationContext"/> backed by one dedicated thread
    /// that pumps a queue of callbacks, mimicking a UI message loop.
    /// </summary>
    private sealed class SingleThreadSynchronizationContext : SynchronizationContext, IDisposable
    {
        private readonly BlockingCollection<(SendOrPostCallback Callback, object? State)> _queue = new();
        private readonly Thread _thread;

        public SingleThreadSynchronizationContext()
        {
            _thread = new Thread(PumpMessages) { IsBackground = true, Name = nameof(SingleThreadSynchronizationContext) };
            _thread.Start();
        }

        public override void Post(SendOrPostCallback d, object? state) => _queue.Add((d, state));

        public override void Send(SendOrPostCallback d, object? state) =>
            throw new NotSupportedException("Synchronous Send is not supported by this context.");

        /// <summary>
        /// Queues <paramref name="action"/> onto the single thread and blocks the caller until it finishes
        /// or <paramref name="timeout"/> elapses. The action itself runs on the dedicated thread, so any
        /// continuation that captures this context can only run if that thread is free to pump — which is
        /// exactly what a sync-over-async call without ConfigureAwait(false) would prevent.
        /// </summary>
        public bool TryRun(Action action, TimeSpan timeout, out Exception? error)
        {
            using var finished = new ManualResetEventSlim(false);
            Exception? captured = null;

            Post(_ =>
            {
                try
                {
                    action();
                }
                catch (Exception e)
                {
                    captured = e;
                }
                finally
                {
                    finished.Set();
                }
            }, null);

            var completed = finished.Wait(timeout);
            error = captured;

            return completed;
        }

        private void PumpMessages()
        {
            SetSynchronizationContext(this);

            foreach (var (callback, state) in _queue.GetConsumingEnumerable())
            {
                callback(state);
            }
        }

        public void Dispose() => _queue.CompleteAdding();
    }
}
