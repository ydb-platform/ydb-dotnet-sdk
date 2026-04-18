namespace Ydb.Sdk.Coordination.Settings;

public class WatchSemaphoreModeUtils
{
    public static bool WatchData(WatchSemaphoreMode mode)
        => (mode == WatchSemaphoreMode.WatchData) | (mode == WatchSemaphoreMode.WatchDataAndOwners);

    public static bool WatchOwners(WatchSemaphoreMode mode)
        => (mode == WatchSemaphoreMode.WatchOwners) | (mode == WatchSemaphoreMode.WatchDataAndOwners);
}
