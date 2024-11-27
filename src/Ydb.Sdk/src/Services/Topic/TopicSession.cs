using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Services.Topic;

internal abstract class TopicSession<TFromClient, TFromServer> : IDisposable
{
    private readonly Func<Task> _initialize;
    private readonly Action<WriterException> _resetSessionOnTransportError;

    protected readonly IBidirectionalStream<TFromClient, TFromServer> Stream;
    protected readonly ILogger Logger;
    protected readonly string SessionId;

    private int _isActive = 1;

    protected TopicSession(
        IBidirectionalStream<TFromClient, TFromServer> stream,
        ILogger logger,
        string sessionId,
        Func<Task> initialize,
        Action<WriterException> resetSessionOnTransportError)
    {
        Stream = stream;
        Logger = logger;
        SessionId = sessionId;
        _initialize = initialize;
        _resetSessionOnTransportError = resetSessionOnTransportError;
    }

    protected async void ReconnectSession(WriterException exception)
    {
        if (Interlocked.CompareExchange(ref _isActive, 0, 1) == 0)
        {
            Logger.LogDebug("Skipping reconnect. A reconnect session has already been initiated");

            return;
        }

        _resetSessionOnTransportError(exception);

        Logger.LogInformation("WriterSession[{SessionId}] has been deactivated, starting to reconnect", SessionId);

        await _initialize();
    }

    public void Dispose()
    {
        Stream.Dispose();
    }
}
