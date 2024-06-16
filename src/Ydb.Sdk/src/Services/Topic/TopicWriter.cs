using Ydb.Sdk.Services.Topic.Internal;
using Ydb.Sdk.Services.Topic.Models.Writer;

namespace Ydb.Sdk.Services.Topic;

public class TopicWriter
{
    private readonly WriterReconnector _writerReconnector;

    internal TopicWriter(WriterReconnector writerReconnector)
    {
        _writerReconnector = writerReconnector;
    }

    public async Task<InitInfo> WaitInit() => await _writerReconnector.WaitInit();

    public async Task<List<WriteResult>> Write(List<Message> messages)
    {
        var resultTasks = await _writerReconnector.Write(messages);
        var results = await Task.WhenAll(resultTasks);
        return results.ToList();
    }

    public async Task Flush() => await _writerReconnector.Flush();

    public async Task Close(bool needFlush = true) => await _writerReconnector.Close(needFlush);
}
