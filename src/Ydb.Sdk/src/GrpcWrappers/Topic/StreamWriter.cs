using Ydb.Sdk.GrpcWrappers.Topic.Writer;
using static Ydb.Topic.StreamWriteMessage.Types;
using InitRequest = Ydb.Sdk.GrpcWrappers.Topic.Writer.Init.InitRequest;
using InitResponse = Ydb.Sdk.GrpcWrappers.Topic.Writer.Init.InitResponse;
using WriteRequest = Ydb.Sdk.GrpcWrappers.Topic.Writer.Write.WriteRequest;
using WriteResponse = Ydb.Sdk.GrpcWrappers.Topic.Writer.Write.WriteResponse;

namespace Ydb.Sdk.GrpcWrappers.Topic;

internal class StreamWriter: IAsyncDisposable
{
    //TODO: common wrapper for both streams
    private readonly WriteMessageResponseStream responseStream;
    private readonly Driver.BidirectionalStream<FromClient, FromServer> stream;

    private StreamWriter(
        WriteMessageResponseStream responseStream,
        Driver.BidirectionalStream<FromClient, FromServer> stream)
    {
        this.responseStream = responseStream;
        this.stream = stream;
    }

    public static async Task<StreamWriter> Init(
        Driver driver,
        InitRequest initRequest)
    {
        var writer = driver.DuplexStreamCall(Ydb.Topic.V1.TopicService.StreamWriteMethod, new GrpcRequestSettings());

        var request = new FromClient
        {
            InitRequest = initRequest.ToProto()
        };
        await writer.Write(request);

        var responseStream = new WriteMessageResponseStream(default/*writer*/); // TODO writer 
        if (!await responseStream.Next())
            throw new Exception("No init response received");
        var initResponse = (InitResponse)responseStream.Response;

        return new StreamWriter(responseStream, writer);
    }

    public async Task Write(IEnumerable<WriteRequest> requests)
    {
        foreach (var request in requests)
        {
            await stream.Write(new FromClient
            {
                WriteRequest = request.ToProto()
            });
        }
    }

    public async Task<WriteResponse> Receive()
    {
        while (true)
        {
            if (!await responseStream.Next())
                continue;
            return (WriteResponse)responseStream.Response;
        }
    }

    public async ValueTask DisposeAsync()
    {
        //TODO
    }
}