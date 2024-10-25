using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Services.Topic;

internal abstract class TopicSession : IDisposable
{
    private readonly Func<Task> _initialize;

    protected readonly ILogger Logger;
    protected readonly string SessionId;

    private int _isActive = 1;
    private bool _disposed;

    protected TopicSession(ILogger logger, string sessionId, Func<Task> initialize)
    {
        Logger = logger;
        SessionId = sessionId;
        _initialize = initialize;
    }

    protected async void ReconnectSession()
    {
        if (Interlocked.CompareExchange(ref _isActive, 0, 1) == 0)
        {
            Logger.LogWarning("Skipping reconnect. A reconnect session has already been initiated");

            return;
        }

        Logger.LogInformation("WriterSession[{SessionId}] has been deactivated, starting to reconnect", SessionId);

        while (!_disposed)
        {
            try
            {
                await _initialize();
                break;
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Unable to reconnect the session due to the following error");
            }
        }
    }

    public void Dispose()
    {
        lock (this)
        {
            _disposed = true;
        }
    }
}
