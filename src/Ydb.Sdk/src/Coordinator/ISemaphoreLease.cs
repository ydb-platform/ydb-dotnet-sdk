namespace Ydb.Sdk.Coordinator;

public interface ISemaphoreLease
{
    string GetSemaphoreName();

    // Временно закоментировал 
    // ICoordinationSession GetSession();

    // Временно закоментировал 
    //Task Release();
}
