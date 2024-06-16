using Microsoft.Extensions.Logging;
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

internal class StreamWriter : IAsyncDisposable
{
    private readonly ICredentialsProvider _credentialsProvider;
    private readonly ILogger<StreamWriter> _logger;

    //TODO: common wrapper for both streams
    private readonly WriteMessageResponseStream _responseStream;
    private readonly Driver.BidirectionalStream<FromClient, FromServer> _stream;
    private readonly CancellationTokenSource _updateTokenCancellationSource = new();
    private readonly TimeSpan? _updateTokenInterval;
    private bool _isClosed;
    private Task? _updateTokenTask;
    private TaskCompletionSource? _updateTokenWaiter;

    private StreamWriter(
        WriteMessageResponseStream responseStream,
        Driver.BidirectionalStream<FromClient, FromServer> stream,
        ICredentialsProvider credentialsProvider,
        ILogger<StreamWriter> logger,
        SupportedCodecs supportedCodecs,
        long lastSequenceNumber,
        TimeSpan? updateTokenInterval)
    {
        _responseStream = responseStream;
        _stream = stream;
        _credentialsProvider = credentialsProvider;
        _logger = logger;
        _updateTokenInterval = updateTokenInterval;
        SupportedCodecs = supportedCodecs;
        LastSequenceNumber = lastSequenceNumber;
    }

    public long LastSequenceNumber { get; }
    public SupportedCodecs SupportedCodecs { get; }

    public async ValueTask DisposeAsync()
    {
        await _stream.DisposeAsync();
        await _responseStream.DisposeAsync();
    }

    public static async Task<StreamWriter> Init(
        Driver driver,
        InitRequest initRequest,
        TimeSpan? updateTokenInterval = null)
    {
        var innerWriter =
            driver.DuplexStreamCall(Ydb.Topic.V1.TopicService.StreamWriteMethod, new GrpcRequestSettings());
        await innerWriter.Write(new FromClient {InitRequest = initRequest.ToProto()});

        var responseStream = new WriteMessageResponseStream(innerWriter);
        await responseStream.Next(); //TODO if returned false
        if (responseStream.Response is not InitResponse initResponse)
        {
            throw new InvalidCastException(
                $"Expected response for init request to be of type {typeof(InitResponse)}, " +
                $"got {responseStream.Response.GetType()}");
        }

        var writer = new StreamWriter(
            responseStream,
            innerWriter,
            driver.CredentialsProvider,
            driver.LoggerFactory.CreateLogger<StreamWriter>(),
            initResponse.Result.SupportedCodecs,
            initResponse.Result.LastSequenceNumber,
            updateTokenInterval);
        if (writer._updateTokenInterval != null)
        {
            writer._updateTokenWaiter = new TaskCompletionSource();
            writer._updateTokenTask = Task.Run(
                async () => await writer.UpdateTokenLoop(writer._updateTokenCancellationSource.Token));
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
                    _logger.LogWarning("Unknown response type: {type}", response.GetType());
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
            _updateTokenCancellationSource.Cancel();
            await _updateTokenTask;
        }

        await DisposeAsync();
    }

    private async Task UpdateTokenLoop(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(_updateTokenInterval!.Value, cancellationToken);
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
