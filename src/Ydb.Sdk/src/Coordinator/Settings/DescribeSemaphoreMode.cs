namespace Ydb.Sdk.Coordinator.Settings;

public enum DescribeSemaphoreMode
{
    DataOnly,
    WithOwners,
    WithWaiters,
    WithOwnersAndWaiters
}
