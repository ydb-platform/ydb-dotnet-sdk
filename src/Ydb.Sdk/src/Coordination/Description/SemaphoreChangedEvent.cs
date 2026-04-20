using Ydb.Coordination;

namespace Ydb.Sdk.Coordination.Description;

public readonly struct SemaphoreChangedEvent
{
    public bool DataChanged { get; }
    public bool OwnersChanged { get; }

    public SemaphoreChangedEvent(SessionResponse.Types.DescribeSemaphoreChanged semaphoreChangedEvent)
    {
        DataChanged = semaphoreChangedEvent.DataChanged;
        OwnersChanged = semaphoreChangedEvent.OwnersChanged;
    }
}
