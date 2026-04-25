namespace Ydb.Sdk.Coordination.Settings;

internal static class WatchSemaphoreModeExtensions
{
    public static bool WatchData(this WatchSemaphoreMode mode) =>
        mode is WatchSemaphoreMode.WatchData
            or WatchSemaphoreMode.WatchDataAndOwners;

    public static bool WatchOwners(this WatchSemaphoreMode mode) =>
        mode is WatchSemaphoreMode.WatchOwners
            or WatchSemaphoreMode.WatchDataAndOwners;
}
