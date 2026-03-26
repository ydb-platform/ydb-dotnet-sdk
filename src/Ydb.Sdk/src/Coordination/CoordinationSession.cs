using Ydb.Sdk.Coordination.Settings;

namespace Ydb.Sdk.Coordination;

public class CoordinationSession
{
    private readonly SessionRuntime _sessionRuntime;

    public CoordinationSession(IDriver driver, string pathNode)
    {
        _sessionRuntime = new SessionRuntime(driver, pathNode);
    }

    public void Status()
    {
    }

    public Semaphore Semaphore(string name)
        => new Semaphore(name, _sessionRuntime);

    public Mutex Mutex(string name)
        => new Mutex(Semaphore(name));

    public Election Election(string name)
        => new Election(Semaphore(name));

    public void Close()
    {
    }
}
