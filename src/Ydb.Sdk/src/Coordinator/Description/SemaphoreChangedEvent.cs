namespace Ydb.Sdk.Coordinator.Description;

using Coordination;

public class SemaphoreChangedEvent
{
    private readonly bool _dataChanged;
    private readonly bool _ownersChanged;

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
