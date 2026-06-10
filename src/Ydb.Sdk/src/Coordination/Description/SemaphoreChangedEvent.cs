using Ydb.Coordination;

namespace Ydb.Sdk.Coordination.Description;

public readonly struct SemaphoreChangedEvent(SessionResponse.Types.DescribeSemaphoreChanged semaphoreChangedEvent)
{
    public bool DataChanged { get; } = semaphoreChangedEvent.DataChanged;
    public bool OwnersChanged { get; } = semaphoreChangedEvent.OwnersChanged;
}
