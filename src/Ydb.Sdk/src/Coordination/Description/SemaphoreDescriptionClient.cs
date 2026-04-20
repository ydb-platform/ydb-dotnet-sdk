using Ydb.Coordination;

namespace Ydb.Sdk.Coordination.Description;

public class SemaphoreDescriptionClient
{
    public string Name { get; }
    public byte[] Data { get; }
    public ulong Count { get; }
    public ulong Limit { get; }
    public bool Ephemeral { get; }
    private readonly IReadOnlyList<Session> _ownersList;
    private readonly IReadOnlyList<Session> _waitersList;

    public SemaphoreDescriptionClient(SemaphoreDescription description)
    {
        Name = description.Name;
        Data = description.Data.ToByteArray();
        Count = description.Count;
        Limit = description.Limit;
        Ephemeral = description.Ephemeral;
        _ownersList = description.Owners?
                          .Select(o => new Session(o))
                          .ToList()
                          .AsReadOnly()
                      ?? new List<Session>().AsReadOnly();

        _waitersList = description.Waiters?
                           .Select(w => new Session(w))
                           .ToList()
                           .AsReadOnly()
                       ?? new List<Session>().AsReadOnly();
    }

    public IReadOnlyList<Session> GetOwnersList() => _ownersList;
    public IReadOnlyList<Session> GetWaitersList() => _waitersList;

    public class Session
    {
        public ulong Id { get; }
        public ulong TimeoutMillis { get; }
        public ulong Count { get; }
        public byte[] Data { get; }
        public ulong OrderId { get; }

        public Session(SemaphoreSession semaphoreSession)
        {
            if (semaphoreSession == null)
                throw new ArgumentNullException(nameof(semaphoreSession));
            Id = semaphoreSession.SessionId;
            TimeoutMillis = semaphoreSession.TimeoutMillis;
            Count = semaphoreSession.Count;
            Data = semaphoreSession.Data.ToByteArray();
            OrderId = semaphoreSession.OrderId;
        }
    }
}
