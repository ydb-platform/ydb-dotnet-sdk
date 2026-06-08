namespace Ydb.Sdk.Coordination.Settings;

internal static class DescribeSemaphoreModeExtensions
{
    public static bool IncludeOwners(this DescribeSemaphoreMode mode) =>
        mode is DescribeSemaphoreMode.WithOwners
            or DescribeSemaphoreMode.WithOwnersAndWaiters;

    public static bool IncludeWaiters(this DescribeSemaphoreMode mode) =>
        mode is DescribeSemaphoreMode.WithWaiters
            or DescribeSemaphoreMode.WithOwnersAndWaiters;
}
