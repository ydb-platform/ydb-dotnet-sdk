namespace Ydb.Sdk.Coordinator;

public interface ISemaphoreLease
{
    string GetSemaphoreName();

    // Временно закоментировал 
   // ICoordinationSession GetSession();

    Task Release();
}
