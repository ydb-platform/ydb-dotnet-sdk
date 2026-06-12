namespace CoordinationService;

public sealed class MonotonicVersionGuard(string observerId)
{
    public string ObserverId { get; } = observerId;

    public long LastObservedVersion { get; private set; } = -1;

    public void Observe(CoordinationPayload payload) => Observe(payload.Version);

    public void Observe(long version)
    {
        if (version < LastObservedVersion)
        {
            throw new CoordinationSloInvariantException(
                $"{ObserverId}: observed coordination version rollback from {LastObservedVersion} to {version}.");
        }

        LastObservedVersion = version;
    }
}

public sealed class CoordinationSloInvariantException : Exception
{
    public CoordinationSloInvariantException(string message) : base(message)
    {
    }

    public CoordinationSloInvariantException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
