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

    private SemaphoreDescription(string name)
    {
        Name = name;
        Data = [];
        OwnersList = [];
        WaitersList = [];
    }

    private SemaphoreDescription(Ydb.Coordination.SemaphoreDescription description)
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

    /// <summary>
    /// Converts a proto description; if <paramref name="description"/> is <c>null</c>
    /// (semaphore does not exist yet), returns an empty placeholder description so observers can
    /// expose "no owner / no value" without crashing.
    /// </summary>
    internal static SemaphoreDescription FromProto(Ydb.Coordination.SemaphoreDescription? description,
        string fallbackName = "") =>
        description is null ? new SemaphoreDescription(fallbackName) : new SemaphoreDescription(description);


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
