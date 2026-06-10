using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;

namespace Ydb.Sdk.Coordination.PrimitiveElection;

public class Election
{
    public string Name { get; }
    private SessionTransport _sessionTransport;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<Election> _logger;

    internal Election(string name, SessionTransport sessionTransport, ILoggerFactory loggerFactory)
    {
        Name = name;
        _sessionTransport = sessionTransport;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<Election>();
    }


    public async Task<Leadership> Campaign(byte[] data, CancellationToken cancellationToken = default)
    {
        var lease = await _sessionTransport.AcquireSemaphore(Name, 1, false, data, null, cancellationToken);
        return new Leadership(Name, _sessionTransport, lease, _loggerFactory);
    }

    public async Task<LeaderInfo?> Leader(CancellationToken cancellationToken = default)
    {
        var description =
            await _sessionTransport.DescribeSemaphore(Name, DescribeSemaphoreMode.WithOwners, cancellationToken);
        var owner = description.OwnersList.FirstOrDefault();
        return owner != null ? new LeaderInfo(owner.Data) : null;
    }


    public async IAsyncEnumerable<LeaderState> Observe(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Observing leadership changes on {Name}", Name);

        LeaderIdentity? previousLeader = null;
        CancellationTokenSource? currentCts = null;

        try
        {
            var watch = await _sessionTransport.WatchSemaphore(Name, DescribeSemaphoreMode.WithOwners,
                WatchSemaphoreMode.WatchOwners, cancellationToken);
            var stateInitial = HandleState(watch.Initial, ref previousLeader, ref currentCts);
            if (stateInitial != null)
            {
                yield return stateInitial;
            }

            await foreach (var description in watch.Updates.WithCancellation(cancellationToken))
            {
                var state = HandleState(description, ref previousLeader, ref currentCts);
                if (state != null)
                {
                    yield return state;
                }
            }
        }
        finally
        {
            if (currentCts != null)
            {
                await currentCts.CancelAsync();
                currentCts.Dispose();
            }

            _logger.LogInformation("Stopped observing leadership changes on {Name}", Name);
        }
    }

    private LeaderState? HandleState(
        SemaphoreDescription description,
        ref LeaderIdentity? previousLeader,
        ref CancellationTokenSource? currentCts)
    {
        var owner = description.OwnersList.FirstOrDefault();

        var currentLeader = owner != null
            ? new LeaderIdentity(owner.Id, owner.OrderId)
            : null;

        if (IsSameLeader(previousLeader, currentLeader))
        {
            return null;
        }

        previousLeader = currentLeader;

        if (currentCts != null)
        {
            currentCts.Cancel();
            currentCts.Dispose();
        }

        currentCts = new CancellationTokenSource();

        if (owner == null)
        {
            _logger.LogInformation("No leader on {Name}", Name);

            return new LeaderState(
                Array.Empty<byte>(),
                false,
                currentCts.Token
            );
        }

        var sessionId = _sessionTransport.SessionId;
        var isMe = owner.Id == sessionId;

        return new LeaderState(
            owner.Data,
            isMe,
            currentCts.Token
        );
    }

    private static bool IsSameLeader(
        LeaderIdentity? left,
        LeaderIdentity? right)
        => Equals(left, right);
}
