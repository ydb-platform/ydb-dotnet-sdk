using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Pool;

internal interface ITcpConnector
{
    Task ConnectAsync(string host, int port, CancellationToken cancellationToken);
}

internal sealed class EndpointLocalDcDetector
{
    private const int MaxEndpointsCheckPerLocation = 5;

    private readonly ILogger<EndpointLocalDcDetector> _logger;
    private readonly ITcpConnector _tcpConnector;

    public EndpointLocalDcDetector(ILoggerFactory loggerFactory, ITcpConnector? tcpConnector = null)
    {
        _logger = loggerFactory.CreateLogger<EndpointLocalDcDetector>();
        _tcpConnector = tcpConnector ?? new SocketTcpConnector();
    }

    public async Task<string?> DetectNearestLocationDc(
        IReadOnlyList<EndpointInfo> endpoints,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);

        if (endpoints.Count == 0)
        {
            throw new ArgumentException("Empty endpoints list for local DC detection", nameof(endpoints));
        }

        var endpointsByLocation = SplitEndpointsByLocation(endpoints);

        _logger.LogDebug(
            "Detecting nearest DC from {EndpointsCount} endpoints across {LocationsCount} locations",
            endpoints.Count,
            endpointsByLocation.Count
        );

        if (endpointsByLocation.Count == 1)
        {
            var location = endpointsByLocation.Keys.Single();
            _logger.LogDebug("Only one location found: {Location}", location);
            return location;
        }

        var endpointsToTest = new List<EndpointInfo>(MaxEndpointsCheckPerLocation * endpointsByLocation.Count);
        foreach (var (location, locationEndpoints) in endpointsByLocation)
        {
            var sample = GetRandomEndpoints(locationEndpoints, MaxEndpointsCheckPerLocation);
            endpointsToTest.AddRange(sample);

            _logger.LogDebug(
                "Selected {SelectedCount}/{LocationEndpointsCount} endpoints from location '{Location}' for testing",
                sample.Count,
                locationEndpoints.Count,
                location
            );
        }

        var fastestEndpoint = await DetectFastestEndpoint(endpointsToTest, timeout, cancellationToken);
        if (fastestEndpoint is null)
        {
            _logger.LogDebug("Failed to detect nearest DC via TCP race: no endpoint connected in time");
            return null;
        }

        _logger.LogDebug("Detected nearest DC: {Location}", fastestEndpoint.LocationDc);
        return fastestEndpoint.LocationDc;
    }

    private async Task<EndpointInfo?> DetectFastestEndpoint(
        IReadOnlyList<EndpointInfo> endpoints,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(timeout);

        var winner = new TaskCompletionSource<EndpointInfo?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = endpoints.Select(endpoint => TryConnect(endpoint, winner, cts)).ToArray();

        try
        {
            await Task.WhenAll(tasks);
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
        }

        if (winner.Task.IsCompletedSuccessfully)
        {
            return await winner.Task;
        }

        return null;
    }

    private async Task TryConnect(
        EndpointInfo endpoint,
        TaskCompletionSource<EndpointInfo?> winner,
        CancellationTokenSource cts)
    {
        try
        {
            await _tcpConnector.ConnectAsync(endpoint.Host, checked((int)endpoint.Port), cts.Token);

            if (winner.TrySetResult(endpoint))
            {
                _logger.LogDebug("TCP race winner endpoint: {Endpoint}", endpoint.Endpoint);
                cts.Cancel();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected: either timeout expired or another endpoint won the race
        }
        catch (SocketException e)
        {
            _logger.LogTrace(e, "TCP race failed for endpoint {Endpoint}", endpoint.Endpoint);
        }
        catch (Exception e)
        {
            _logger.LogDebug(e, "Unexpected error during TCP race for endpoint {Endpoint}", endpoint.Endpoint);
        }
    }

    private static Dictionary<string, List<EndpointInfo>> SplitEndpointsByLocation(
        IReadOnlyList<EndpointInfo> endpoints)
    {
        var result = new Dictionary<string, List<EndpointInfo>>(StringComparer.Ordinal);
        foreach (var endpoint in endpoints)
        {
            if (!result.TryGetValue(endpoint.LocationDc, out var locationEndpoints))
            {
                locationEndpoints = [];
                result[endpoint.LocationDc] = locationEndpoints;
            }

            locationEndpoints.Add(endpoint);
        }

        return result;
    }

    private static IReadOnlyList<EndpointInfo> GetRandomEndpoints(IReadOnlyList<EndpointInfo> endpoints, int count)
    {
        if (endpoints.Count <= count)
        {
            return endpoints;
        }

        return endpoints
            .OrderBy(_ => Random.Shared.Next())
            .Take(count)
            .ToArray();
    }
}

internal sealed class SocketTcpConnector : ITcpConnector
{
    public async Task ConnectAsync(string host, int port, CancellationToken cancellationToken)
    {
        var addresses = await Dns.GetHostAddressesAsync(host, cancellationToken);
        if (addresses.Length == 0)
        {
            throw new SocketException((int)SocketError.HostNotFound);
        }

        SocketException? lastException = null;
        foreach (var address in addresses)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                using var socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                await socket.ConnectAsync(new IPEndPoint(address, port), cancellationToken);
                return;
            }
            catch (SocketException e)
            {
                lastException = e;
            }
        }

        throw lastException!;
    }
}
