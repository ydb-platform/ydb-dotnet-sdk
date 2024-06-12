using Ydb.Sdk.Services.Topic.Internal;
using Ydb.Sdk.Services.Topic.Models.Writer;

namespace Ydb.Sdk.Services.Topic;

public class TopicWriter: IDisposable, IAsyncDisposable
{
    private readonly WriterReconnector writerReconnector;

    internal TopicWriter(WriterReconnector writerReconnector)
    {
        this.writerReconnector = writerReconnector;
    }

    public async Task<InitInfo> WaitInit() => await writerReconnector.WaitInit();

    public async Task<List<Task<WriteResult>>> Write(List<Message> message) => await writerReconnector.Write(message);

    public async Task Flush() => await writerReconnector.Flush();

    public void Dispose()
    {
        throw new NotImplementedException();
    }

    public async ValueTask DisposeAsync()
    {
        throw new NotImplementedException();
    }
}
