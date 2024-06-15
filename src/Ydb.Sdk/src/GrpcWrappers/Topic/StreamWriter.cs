using Ydb.Sdk.Auth;
using Ydb.Sdk.GrpcWrappers.Topic.Codecs;
using Ydb.Sdk.GrpcWrappers.Topic.Writer;
using Ydb.Sdk.GrpcWrappers.Topic.Writer.UpdateToken;
using static Ydb.Topic.StreamWriteMessage.Types;
using InitRequest = Ydb.Sdk.GrpcWrappers.Topic.Writer.Init.InitRequest;
using InitResponse = Ydb.Sdk.GrpcWrappers.Topic.Writer.Init.InitResponse;
using WriteRequest = Ydb.Sdk.GrpcWrappers.Topic.Writer.Write.WriteRequest;
using WriteResponse = Ydb.Sdk.GrpcWrappers.Topic.Writer.Write.WriteResponse;

namespace Ydb.Sdk.GrpcWrappers.Topic;

internal class StreamWriter: IAsyncDisposable
{
    private TaskCompletionSource? _updateTokenWaiter;
    private Task? _updateTokenTask;
    private bool _isClosed;

    //TODO: common wrapper for both streams
    private readonly WriteMessageResponseStream _responseStream;
    private readonly Driver.BidirectionalStream<FromClient, FromServer> _stream;
    private readonly TimeSpan? _updateTokenInterval;
    private readonly ICredentialsProvider _credentialsProvider;

    private StreamWriter(
        WriteMessageResponseStream responseStream,
        Driver.BidirectionalStream<FromClient, FromServer> stream,
        ICredentialsProvider credentialsProvider,
        TimeSpan? updateTokenInterval)
    {
        _responseStream = responseStream;
        _stream = stream;
        _credentialsProvider = credentialsProvider;
        _updateTokenInterval = updateTokenInterval;
    }

    public long LastSequenceNumber { get; private set; }
    public SupportedCodecs? SupportedCodecs { get; private set; }

    public static async Task<StreamWriter> Init(
        Driver driver,
        InitRequest initRequest,
        TimeSpan? updateTokenInterval = null)
    {
        var innerWriter = driver.DuplexStreamCall(Ydb.Topic.V1.TopicService.StreamWriteMethod, new GrpcRequestSettings());
        await innerWriter.Write(new FromClient {InitRequest = initRequest.ToProto()});

        var responseStream = new WriteMessageResponseStream(innerWriter); 
        if (!await responseStream.Next()) ;
            //TODO
        if (responseStream.Response is not InitResponse initResponse)
        {
            throw new InvalidCastException(
                $"Expected response for init request to be of type {typeof(InitResponse)}, " +
                $"got {responseStream.Response.GetType()}");
        }

        var writer = new StreamWriter(responseStream, innerWriter, driver.CredentialsProvider, updateTokenInterval);
        writer.LastSequenceNumber = initResponse.Result.LastSequenceNumber;
        writer.SupportedCodecs = initResponse.Result.SupportedCodecs;
        if (writer._updateTokenInterval != null)
        {
            writer._updateTokenWaiter = new TaskCompletionSource();
            writer._updateTokenTask = Task.Run(writer.UpdateTokenLoop);
        }

        return writer;
    }

    public async Task Write(WriteRequest request)
    {
        await _stream.Write(new FromClient
        {
            WriteRequest = request.ToProto()
        });
    }

    public async Task<WriteResponse> Receive()
    {
        while (true)
        {
            if (!await _responseStream.Next())
                continue;
            var response = _responseStream.Response;
            switch (response)
            {
                case UpdateTokenResponse:
                    _updateTokenWaiter = new TaskCompletionSource();
                    break;
                case WriteResponse writeResponse:
                    return writeResponse;
                default:
                    log
                    break;
            }
        }
    }

    public async Task Close()
    {
        if (_isClosed)
            return;
        _isClosed = true;
        if (_updateTokenTask != null)
        {
            _updateTokenTask;
            await _updateTokenTask;
        }
        await DisposeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _stream.DisposeAsync();
        await _responseStream.DisposeAsync();
    }

    private async Task UpdateTokenLoop()
    {
        while (true)
        {
            await Task.Delay(_updateTokenInterval!.Value);
            await UpdateToken(_credentialsProvider.GetAuthInfo() ?? "");
        }
    }

    private async Task UpdateToken(string token)
    {
        await _updateTokenWaiter!.Task;
        try
        {
            var request = new UpdateTokenRequest {Token = token}.ToClientRequest();
            await _stream.Write(request);
        }
        finally
        {
            _updateTokenWaiter.SetResult();
        }
    }
}