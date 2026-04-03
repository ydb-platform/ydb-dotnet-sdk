using Ydb.Coordination;

namespace Ydb.Sdk.Coordination.Description;

public readonly struct SemaphoreChangedEvent
{
    private readonly bool _dataChanged;
    private readonly bool _ownersChanged;

    public SemaphoreChangedEvent(SessionResponse.Types.DescribeSemaphoreChanged semaphoreChangedEvent)
    {
        _dataChanged = semaphoreChangedEvent.DataChanged;
        _ownersChanged = semaphoreChangedEvent.OwnersChanged;
    }
    
    public SemaphoreChangedEvent(bool dataChanged, bool ownersChanged)
    {
        _dataChanged = dataChanged;
        _ownersChanged =  ownersChanged;
    }

    public bool IsDataChanged()
        => _dataChanged;

    public bool OwnersChanged()
        => _ownersChanged;
}
