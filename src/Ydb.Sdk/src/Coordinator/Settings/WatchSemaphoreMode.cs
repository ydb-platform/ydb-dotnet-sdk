namespace Ydb.Sdk.Coordinator.Settings;

public enum WatchSemaphoreMode
{
    WatchData,

    /**
     * Watch for changes in semaphore owners
     */
    WatchOwners,

    /**
     * Watch for changes in semaphore data or owners
     */
    WatchDataAndOwners
}
