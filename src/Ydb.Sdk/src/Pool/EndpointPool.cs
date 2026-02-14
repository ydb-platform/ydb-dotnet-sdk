using System.Collections.Immutable;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Ado.Internal;

namespace Ydb.Sdk.Pool;

internal class EndpointPool
{
    private const int DiscoveryDegradationLimit = 50;

    private readonly ILogger<EndpointPool> _logger;
    private readonly ReaderWriterLockSlim _rwLock = new();
    private readonly IRandom _random;

    // [0, 0, 0, int.Max, int.Max]
    private IReadOnlyList<EndpointInfo> _sortedByPriorityEndpoints = ImmutableArray<EndpointInfo>.Empty;
    private Dictionary<long, EndpointInfo> _nodeIdToEndpoint = new();
    private int _preferredEndpointCount;

    internal EndpointPool(ILoggerFactory loggerFactory, IRandom? random = null)
    {
        _logger = loggerFactory.CreateLogger<EndpointPool>();
        _random = random ?? ThreadLocalRandom.Instance;
    }

    public EndpointInfo GetEndpoint(long nodeId = 0)
    {
        _rwLock.EnterReadLock();
        try
        {
            return nodeId > 0 && _nodeIdToEndpoint.TryGetValue(nodeId, out var endpoint)
                ? endpoint
                : _sortedByPriorityEndpoints[_random.Next(_preferredEndpointCount)];
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    public ImmutableArray<EndpointInfo> Reset(IReadOnlyList<EndpointInfo> endpointSettingsList)
    {
        Dictionary<long, EndpointInfo> nodeIdToEndpoint = new();
        HashSet<string> newEndpoints = new();

        _logger.LogDebug("Init endpoint pool with {EndpointLength} endpoints", endpointSettingsList.Count);

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
                nodeIdToEndpoint.Add(endpointSettings.NodeId, endpointSettings);
            }
        }

        _rwLock.EnterWriteLock();
        try
        {
            var removed = _sortedByPriorityEndpoints
                .Where(priorityEndpoint => !newEndpoints.Contains(priorityEndpoint.Endpoint))
                .ToImmutableArray();

            _sortedByPriorityEndpoints = endpointSettingsList;
            _preferredEndpointCount = endpointSettingsList.Count;
            _nodeIdToEndpoint = nodeIdToEndpoint;

            return removed;
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    // return needDiscovery 
    public bool PessimizeEndpoint(EndpointInfo endpointInfo)
    {
        var knownEndpoint = _sortedByPriorityEndpoints.FirstOrDefault(pe => endpointInfo.NodeId == pe.NodeId);

        if (knownEndpoint == null)
        {
            return false;
        }

        if (knownEndpoint.IsPessimized)
        {
            _logger.LogTrace("Endpoint {Endpoint} is already pessimized", endpointInfo.Endpoint);

            return false; // if we got a twice pessimized node, that is, the first time with an accurate need discovery 
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

            _logger.LogWarning(
                "Endpoint {Endpoint} was pessimized. New pessimization ratio: {PessimizedCount} / {EndpointsCount}",
                endpointInfo.Endpoint, pessimizedCount, _sortedByPriorityEndpoints.Count);

            return 100 * pessimizedCount > _sortedByPriorityEndpoints.Count * DiscoveryDegradationLimit;
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }
}

public record EndpointInfo(long NodeId, bool Ssl, string Host, uint Port, string LocationDc)
{
    internal string Endpoint { get; } = $"{(Ssl ? "https" : "http")}://{Host}:{Port}";

    internal int Priority { get; private set; }

    internal void Pessimize() => Priority = int.MaxValue;

    internal bool IsPessimized => Priority == int.MaxValue;
};
