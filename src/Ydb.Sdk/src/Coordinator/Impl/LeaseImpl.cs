namespace Ydb.Sdk.Coordinator.Impl;

public class LeaseImpl : ISemaphoreLease
{
    private readonly SessionImpl _session;
    private readonly String _name;

    public LeaseImpl(SessionImpl session, string name)
    {
        _session = session;
        _name = name;
    }

    public ICoordinationSession GetSession() => _session;


    public string GetSemaphoreName()
        => _name;

    public Task Release()
    {
        // дописать
        return null;
    }
}
