using System.Collections.Concurrent;
using System.Threading.Channels;
using Ydb.Sdk.Coordination.Description;

namespace Ydb.Sdk.Coordination.Watcher;

public class WatchSubscription : IDisposable
{
    private string Name { get; }
    public ulong ReqId { get; set; }

    private bool _isClosed = false;

    private readonly Channel<SemaphoreChangedEvent> _channel =
        Channel.CreateUnbounded<SemaphoreChangedEvent>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });

    internal WatchSubscription(string name)
    {
        this.Name = name;
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
    {
        return _channel.Reader.ReadAllAsync(ct);
    }
    
    public void Dispose()
    {
        _isClosed = true;
        _channel.Writer.Complete();
    }
}
