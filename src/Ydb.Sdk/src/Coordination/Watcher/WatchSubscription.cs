using System.Threading.Channels;
using Ydb.Sdk.Coordination.Description;

namespace Ydb.Sdk.Coordination.Watcher;

public class WatchSubscription : IDisposable
{
    public ulong ReqId { get; set; }

    private volatile bool _isClosed;

    private readonly Channel<SemaphoreChangedEvent> _channel =
        Channel.CreateUnbounded<SemaphoreChangedEvent>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true
            }
        );


    public void Push(
        SemaphoreChangedEvent item)
    {
        if (!_isClosed)
            _channel.Writer.TryWrite(item);
    }

    public IAsyncEnumerable<SemaphoreChangedEvent> ReadAllAsync(CancellationToken ct = default)
        => _channel.Reader.ReadAllAsync(ct);

    public void Dispose()
    {
        if (_isClosed)
            return;
        try
        {
            _isClosed = true;
            _channel.Writer.Complete();
        }
        finally
        {
            GC.SuppressFinalize(this);
        }
    }
}
