using System.Collections.Immutable;
using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Pool;

public class EndpointPool
{
    private const int DiscoveryDegradationLimit = 50;

    private readonly ILogger<EndpointPool> _logger;
    private readonly ReaderWriterLockSlim _rwLock = new();
    private readonly IRandom _random;

    // [0, 0, 0, int.Max, int.Max]
    private ImmutableArray<PriorityEndpoint> _sortedByPriorityEndpoints = ImmutableArray<PriorityEndpoint>.Empty;
    private Dictionary<int, string> _nodeIdToEndpoint = new();
    private int _preferredEndpointCount;

    public EndpointPool(ILogger<EndpointPool> logger, IRandom? random = null)
    {
        _logger = logger;
        random ??= ThreadLocalRandom.Instance;

        _random = random;
    }

    public string GetEndpoint(int nodeId = 0)
    {
        _rwLock.EnterReadLock();
        try
        {
            return nodeId > 0 && _nodeIdToEndpoint.TryGetValue(nodeId, out var endpoint)
                ? endpoint
                : _sortedByPriorityEndpoints[_random.Next(_preferredEndpointCount)].Endpoint;
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    public ImmutableArray<string> Reset(ImmutableArray<EndpointSettings> endpointSettingsList)
    {
        Dictionary<int, string> nodeIdToEndpoint = new();
        HashSet<string> newEndpoints = new();

        _logger.LogDebug("Init endpoint pool with {EndpointLength} endpoints", endpointSettingsList.Length);

        foreach (var endpointSettings in endpointSettingsList)
        {
            _logger.LogDebug("Registered endpoint: {Endpoint}", endpointSettings.Endpoint);

            if (!newEndpoints.Add(endpointSettings.Endpoint))
            {
                _logger.LogWarning("Duplicate endpoint: {Endpoint}", endpointSettings.Endpoint);

                continue;
            }

            if (endpointSettings.NodeId != 0) // NodeId == 0 - serverless proxy
            {
                nodeIdToEndpoint.Add(endpointSettings.NodeId, endpointSettings.Endpoint);
            }
        }

        _rwLock.EnterWriteLock();
        try
        {
            var removed = _sortedByPriorityEndpoints
                .Where(priorityEndpoint => !newEndpoints.Contains(priorityEndpoint.Endpoint))
                .Select(priorityEndpoint => priorityEndpoint.Endpoint)
                .ToImmutableArray();

            _sortedByPriorityEndpoints = endpointSettingsList
                .Select(settings => new PriorityEndpoint(settings.Endpoint))
                .ToImmutableArray();
            _preferredEndpointCount = endpointSettingsList.Length;
            _nodeIdToEndpoint = nodeIdToEndpoint;

            return removed;
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    // return needDiscovery 
    public bool PessimizeEndpoint(string endpoint)
    {
        var knownEndpoint = _sortedByPriorityEndpoints.FirstOrDefault(pe => pe.Endpoint == endpoint);

        if (knownEndpoint == null)
        {
            return false;
        }

        if (knownEndpoint.IsPessimized)
        {
            _logger.LogTrace("Endpoint {Endpoint} is already pessimized", endpoint);

            return false;
        }

        _rwLock.EnterWriteLock();
        try
        {
            knownEndpoint.Pessimize();

            _sortedByPriorityEndpoints = _sortedByPriorityEndpoints
                .OrderBy(priorityEndpoint => priorityEndpoint.Priority)
                .ThenBy(priorityEndpoint => priorityEndpoint.Endpoint)
                .ToImmutableArray();

            var bestPriority = _sortedByPriorityEndpoints[0].Priority;
            var preferredEndpointCount = 0;
            var pessimizedCount = 0;

            foreach (var priorityEndpoint in _sortedByPriorityEndpoints)
            {
                if (priorityEndpoint.Priority == bestPriority)
                {
                    preferredEndpointCount++;
                }

                if (priorityEndpoint.IsPessimized)
                {
                    pessimizedCount++;
                }
            }

            _preferredEndpointCount = preferredEndpointCount;

            _logger.LogTrace("Endpoint {Endpoint} was pessimized. New pessimization ratio: {} / {}",
                endpoint, pessimizedCount, _sortedByPriorityEndpoints.Length);

            return 100 * pessimizedCount > _sortedByPriorityEndpoints.Length * DiscoveryDegradationLimit;
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    private record PriorityEndpoint(string Endpoint)
    {
        internal int Priority { get; private set; }

        internal void Pessimize()
        {
            Priority = int.MaxValue;
        }

        internal bool IsPessimized => Priority == int.MaxValue;
    };
}

public record EndpointSettings(int NodeId, string Endpoint, string LocationDc);

public interface IRandom
{
    public int Next(int maxValue);
}

internal class ThreadLocalRandom : IRandom
{
    internal static readonly ThreadLocalRandom Instance = new();

    [ThreadStatic] private static Random? _random;

    private static Random ThreadStaticRandom => _random ??= new Random();

    private ThreadLocalRandom()
    {
    }

    public int Next(int maxValue)
    {
        return ThreadStaticRandom.Next(maxValue);
    }
}
