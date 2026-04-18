using Ydb.Coordination;

namespace Ydb.Sdk.Coordination.Description;

public readonly struct SemaphoreChangedEvent
{
    private readonly bool _dataChanged = false;
    private readonly bool _ownersChanged = false;

    public SemaphoreChangedEvent(SessionResponse.Types.DescribeSemaphoreChanged semaphoreChangedEvent)
    {
        _dataChanged = semaphoreChangedEvent.DataChanged;
        _ownersChanged = semaphoreChangedEvent.OwnersChanged;
    }

    public bool IsDataChanged()
        => _dataChanged;

    public bool OwnersChanged()
        => _ownersChanged;
}
