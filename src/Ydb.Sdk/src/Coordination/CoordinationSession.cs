using Ydb.Sdk.Coordination.Settings;

namespace Ydb.Sdk.Coordination;

public class CoordinationSession
{
    private readonly SessionRuntime _sessionRuntime;

    public CoordinationSession(IDriver driver, string pathNode)
    {
        _sessionRuntime = new SessionRuntime(driver, pathNode);
    }

    public StateSession Status() => StateSession.Closed;

    public Semaphore Semaphore(string name) => new(name, _sessionRuntime);

    public Mutex Mutex(string name) => new(Semaphore(name));

    public Election Election(string name) => new(Semaphore(name));

    public async Task Close()
        => await _sessionRuntime.Dispose();
}
