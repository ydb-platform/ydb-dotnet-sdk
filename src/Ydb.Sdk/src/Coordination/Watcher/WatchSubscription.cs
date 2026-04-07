using System.Threading.Channels;
using Ydb.Sdk.Coordination.Description;

namespace Ydb.Sdk.Coordination.Watcher;

public class WatchSubscription : IDisposable
{
    public ulong ReqId { get; set; }

    private bool _isClosed;

    private readonly Channel<SemaphoreChangedEvent> _channel =
        Channel.CreateBounded<SemaphoreChangedEvent>(new BoundedChannelOptions(1)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.DropOldest
        });

    public WatchSubscription()
    {
        ReqId = 0L;
    }


    public void Push(SemaphoreChangedEvent item)
    {
        if (!_isClosed)
        {
            _channel.Writer.TryWrite(item);
        }
    }

    public IAsyncEnumerable<SemaphoreChangedEvent> ReadAllAsync(CancellationToken ct = default)
        => _channel.Reader.ReadAllAsync(ct);


    // coalescing helper
    public void Drain()
    {
        while (_channel.Reader.TryRead(out _))
        {
        }
    }

    public void Dispose()
    {
        _isClosed = true;
        _channel.Writer.Complete();
    }
}
