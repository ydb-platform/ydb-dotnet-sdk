using System.Threading.Channels;
using Ydb.Sdk.Coordination.Description;

namespace Ydb.Sdk.Coordination.Internal;

internal sealed class WatchSubscription(string name) : IDisposable
{
    public string Name { get; } = name;

    public ulong ReqId { get; set; }

    private readonly Channel<SemaphoreChangedEvent> _events =
        Channel.CreateUnbounded<SemaphoreChangedEvent>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });

    private volatile bool _closed;

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
