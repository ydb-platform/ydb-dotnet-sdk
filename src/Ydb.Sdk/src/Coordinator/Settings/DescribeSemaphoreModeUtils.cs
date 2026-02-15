namespace Ydb.Sdk.Coordinator.Settings;

public class DescribeSemaphoreModeUtils
{
    public static bool IncludeOwners(DescribeSemaphoreMode mode)
        => (mode == DescribeSemaphoreMode.WithOwners) | (mode == DescribeSemaphoreMode.WithOwnersAndWaiters);

    public static bool IncludeWaiters(DescribeSemaphoreMode mode)
        => (mode == DescribeSemaphoreMode.WithWaiters) | (mode == DescribeSemaphoreMode.WithOwnersAndWaiters);
}
