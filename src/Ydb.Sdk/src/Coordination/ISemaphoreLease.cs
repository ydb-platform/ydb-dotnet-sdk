namespace Ydb.Sdk.Coordination;

public interface ISemaphoreLease
{
    string GetSemaphoreName();

    // Временно закоментировал 
    // ICoordinationSession GetSession();

    // Временно закоментировал 
    //Task Release();
}
