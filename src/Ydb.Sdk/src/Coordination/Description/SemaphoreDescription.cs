using Ydb.Coordination;

namespace Ydb.Sdk.Coordination.Description;

public class SemaphoreDescription
{
    public string Name { get; }
    public byte[] Data { get; }
    public ulong Count { get; }
    public ulong Limit { get; }
    public bool Ephemeral { get; }
    public IReadOnlyList<Session> OwnersList { get; }
    public IReadOnlyList<Session> WaitersList { get; }

    internal SemaphoreDescription(Ydb.Coordination.SemaphoreDescription description)
    {
        Name = description.Name;
        Data = description.Data.ToByteArray();
        Count = description.Count;
        Limit = description.Limit;
        Ephemeral = description.Ephemeral;
        OwnersList = description.Owners?
            .Select(o => new Session(o))
            .ToList() ?? [];

        WaitersList = description.Waiters?
            .Select(w => new Session(w))
            .ToList() ?? [];
    }

    public class Session(SemaphoreSession semaphoreSession)
    {
        public ulong Id { get; } = semaphoreSession.SessionId;
        public ulong TimeoutMillis { get; } = semaphoreSession.TimeoutMillis;
        public ulong Count { get; } = semaphoreSession.Count;
        public byte[] Data { get; } = semaphoreSession.Data.ToByteArray();
        public ulong OrderId { get; } = semaphoreSession.OrderId;
    }
}
