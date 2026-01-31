using Ydb.Coordination;

namespace Ydb.Sdk.Coordinator.Description;

public class SemaphoreDescription
{
    public string Name { get; }
    public byte[] Data { get; }
    public ulong Count { get; }
    public ulong Limit { get; }
    public bool Ephemeral { get; }
    private readonly List<Session> _ownersList;
    private readonly List<Session> _waitersList;
    
    public SemaphoreDescription(Coordination.SemaphoreDescription description)
    {
        Name = description.Name;
        Data = description.Data.ToByteArray();
        Count = description.Count;
        Limit = description.Limit;
        Ephemeral = description.Ephemeral;
        _ownersList = new List<Session>();
        _waitersList = new List<Session>();

        foreach (var owner in description.Owners)
        {
            _ownersList.Add(new Session(owner));
        }

        foreach (var waiters in description.Waiters)
        {
            _waitersList.Add(new Session(waiters));
        }
    }
    
    public List<Session> GetOwnersList() => _ownersList;
    public List<Session> GetWaitersList() => _waitersList;
    
    public class Session
    {
        public ulong Id { get; }
        public ulong TimeoutMillis { get; }
        public ulong Count { get; }
        public byte[] Data { get; }
        public ulong OrderId { get; }

        public Session(SemaphoreSession semaphoreSession)
        {
            Id = semaphoreSession.SessionId;
            TimeoutMillis = semaphoreSession.TimeoutMillis;
            Count = semaphoreSession.Count;
            Data = semaphoreSession.Data.ToByteArray();
            OrderId = semaphoreSession.OrderId;
        }
    }
}
