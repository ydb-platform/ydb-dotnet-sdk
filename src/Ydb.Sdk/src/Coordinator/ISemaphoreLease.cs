namespace Ydb.Sdk.Coordinator;

public interface ISemaphoreLease
{
    string GetSemaphoreName();

    ICoordinationSession GetSession();

    Task Release();
}
