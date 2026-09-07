using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Topic;

internal abstract class TopicSession<TFromClient, TFromServer>(
    IBidirectionalStream<TFromClient, TFromServer> stream,
    ILogger logger,
    string sessionId,
    string? lastToken
) : IAsyncDisposable
{
    protected readonly IBidirectionalStream<TFromClient, TFromServer> Stream = stream;
    protected readonly ILogger Logger = logger;
    protected readonly string SessionId = sessionId;

    private int _isActive = 1;
    private string? _lastToken = lastToken;

    public bool IsActive => Volatile.Read(ref _isActive) == 1;

    protected void ReconnectSession(StatusCode statusCode = StatusCode.Unspecified)
    {
        if (Interlocked.CompareExchange(ref _isActive, 0, 1) == 0)
        {
            Logger.LogDebug("Skipping reconnect. A reconnect session has already been initiated");

            return;
        }

        Logger.LogDebug("TopicSession[{SessionId}] has been deactivated, starting to reconnect", SessionId);

        InternalReconnect(statusCode);
    }

    protected abstract void InternalReconnect(StatusCode statusCode);

    protected async Task SendMessage(TFromClient fromClient)
    {
        var curAuthToken = await Stream.AuthToken().ConfigureAwait(false);

        if (!string.Equals(_lastToken, curAuthToken) && curAuthToken != null)
        {
            var updateTokenRequest = GetSendUpdateTokenRequest(curAuthToken);

            _lastToken = curAuthToken;

            await Stream.Write(updateTokenRequest).ConfigureAwait(false);
        }

        await Stream.Write(fromClient).ConfigureAwait(false);
    }

    protected abstract TFromClient GetSendUpdateTokenRequest(string token);

    public abstract ValueTask DisposeAsync();
}
