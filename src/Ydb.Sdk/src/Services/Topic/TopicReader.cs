using Ydb.Sdk.Services.Topic.Internal;
using Ydb.Sdk.Services.Topic.Models.Reader;

namespace Ydb.Sdk.Services.Topic;

public class TopicReader
{
    private readonly ReaderReconnector readerReconnector;

    internal TopicReader(ReaderReconnector readerReconnector)
    {
        this.readerReconnector = readerReconnector;
    }

    public async Task<Message?> ReceiveMessage()
    {
        await WaitMessage();
        return await readerReconnector.ReceiveMessage();
    }

    public async Task<Batch> ReceiveBatch()
    {
        await WaitMessage();
        return await readerReconnector.ReceiveBatch();
    }

    public async Task Commit()
    {
        
    }

    public async Task WaitMessage() => await readerReconnector.WaitMessage();
}
