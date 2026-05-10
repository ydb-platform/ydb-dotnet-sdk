namespace Ydb.Sdk.Coordination.Settings;

public enum DescribeSemaphoreMode
{
    DataOnly,
    WithOwners,
    WithWaiters,
    WithOwnersAndWaiters
}
