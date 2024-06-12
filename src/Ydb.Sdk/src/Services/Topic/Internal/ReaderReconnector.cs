using Ydb.Sdk.Services.Topic.Models.Reader;
using StreamReader = Ydb.Sdk.GrpcWrappers.Topic.StreamReader;

namespace Ydb.Sdk.Services.Topic.Internal;

internal class ReaderReconnector: IDisposable, IAsyncDisposable
{
    private static int lastInstanceNumber;

    private readonly int id;
    private readonly ReaderConfig config;
    private readonly Driver driver;
    private readonly List<Task> tasks = new();

    private StreamReader? streamReader;

    public ReaderReconnector(Driver driver, ReaderConfig config)
    {
        id = Interlocked.Increment(ref lastInstanceNumber);
        this.driver = driver;
        this.config = config;
    }

    public async Task ConnectionLoop()
    {
        var attempt = 0;
        while (true)
        {
            try
            {
                attempt = 0;
                streamReader = await StreamReader.Create(id, driver, config);
            }
            catch (Exception e)
            {
                attempt++;
            }
        }
    }

    public async Task WaitMessage()
    {
        while (true)
        {
            if (streamReader != null)
            {
                try
                {
                    await streamReader.WaitMessage();
                    return;
                }
                catch (Exception)
                {
                }
            }
        }
    }

    public async Task<Batch> ReceiveBatch() => throw new NotImplementedException(); //await streamReader!.ReceiveBatch();

    public async Task<Message?> ReceiveMessage() => throw new NotImplementedException(); //await streamReader!.ReceiveMessage();

    public async Task Commit(CommitRange commitRange) => await streamReader.Commit(commitRange);

    public void Dispose()
    {
        throw new NotImplementedException();
    }

    public ValueTask DisposeAsync()
    {
        throw new NotImplementedException();
    }
}
