using System.Threading.Channels;
using Ydb.Sdk.Coordination.Description;

namespace Ydb.Sdk.Coordination.Internal;

/// <summary>
/// Push channel used by <see cref="WatcherRegistry"/> to deliver <see cref="SemaphoreChangedEvent"/>
/// notifications from the session reader loop to the watcher consumer.
/// </summary>
internal sealed class WatchSubscription : IDisposable
{
    public string Name { get; }

    /// <summary>The reqId of the most recent <c>DescribeSemaphore</c> with <c>WatchAdded=true</c>.</summary>
    public ulong ReqId { get; set; }

    private readonly Channel<SemaphoreChangedEvent> _events =
        Channel.CreateUnbounded<SemaphoreChangedEvent>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });

    private volatile bool _closed;

    public WatchSubscription(string name)
    {
        Name = name;
    }

    public void Push(SemaphoreChangedEvent evt)
    {
        if (_closed)
            return;

        _events.Writer.TryWrite(evt);
    }

    public IAsyncEnumerable<SemaphoreChangedEvent> ReadAllAsync(CancellationToken cancellationToken = default)
        => _events.Reader.ReadAllAsync(cancellationToken);

    public void Dispose()
    {
        if (_closed)
            return;

        _closed = true;
        _events.Writer.TryComplete();
    }
}
