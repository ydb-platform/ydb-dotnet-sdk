using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Services.Topic;

internal abstract class TopicSession : IDisposable
{
    private readonly Func<Task> _initialize;
 
    protected readonly ILogger Logger;
    protected readonly string SessionId;
    
    private int _isActive = 1;

    protected TopicSession(ILogger logger, string sessionId, Func<Task> initialize)
    {
        Logger = logger;
        _initialize = initialize;
        SessionId = sessionId;
    }

    protected async void ReconnectSession()
    {
        if (Interlocked.CompareExchange(ref _isActive, 0, 1) == 0)
        {
            return;    
        }
        
        while (true)
        {
            try
            {
                await _initialize();
                break;
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Error reconnect session!");
            }
        }
    }
    
    public void Dispose()
    {
        
    }
}
