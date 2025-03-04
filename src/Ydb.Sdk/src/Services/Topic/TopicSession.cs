using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Services.Topic;

internal abstract class TopicSession<TFromClient, TFromServer> : IAsyncDisposable
{
    private readonly Func<Task> _initialize;

    protected readonly IBidirectionalStream<TFromClient, TFromServer> Stream;
    protected readonly ILogger Logger;
    protected readonly string SessionId;

    private int _isActive = 1;
    private string? _lastToken;

    protected TopicSession(
        IBidirectionalStream<TFromClient, TFromServer> stream,
        ILogger logger,
        string sessionId,
        Func<Task> initialize)
    {
        Stream = stream;
        Logger = logger;
        SessionId = sessionId;
        _initialize = initialize;
        _lastToken = stream.AuthToken;
    }

    public bool IsActive => Volatile.Read(ref _isActive) == 1;

    protected async void ReconnectSession()
    {
        if (Interlocked.CompareExchange(ref _isActive, 0, 1) == 0)
        {
            Logger.LogDebug("Skipping reconnect. A reconnect session has already been initiated");

            return;
        }

        Logger.LogInformation("TopicSession[{SessionId}] has been deactivated, starting to reconnect", SessionId);

        await _initialize();
    }

    protected async Task SendMessage(TFromClient fromClient)
    {
        var curAuthToken = Stream.AuthToken;

        if (!string.Equals(_lastToken, curAuthToken) && curAuthToken != null)
        {
            var updateTokenRequest = GetSendUpdateTokenRequest(curAuthToken);

            _lastToken = curAuthToken;

            await Stream.Write(updateTokenRequest);
        }

        await Stream.Write(fromClient);
    }

    protected abstract TFromClient GetSendUpdateTokenRequest(string token);

    public abstract ValueTask DisposeAsync();
}
