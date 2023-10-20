using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Net.Security;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Org.BouncyCastle.Security;
using Ydb.Discovery;

namespace Ydb.Sdk;

internal class ChannelsCache : IDisposable
{
    private readonly DriverConfig _driverConfig;
    private readonly ILogger _logger;
    private readonly object _updateLock = new();
    private EndpointsData _endpointsData;
    private bool _disposed;

    private readonly Random _random = new();

    public ChannelsCache(DriverConfig driverConfig, ILoggerFactory loggerFactory)
    {
        _driverConfig = driverConfig;
        _logger = loggerFactory.CreateLogger<ChannelsCache>();
        _endpointsData = new EndpointsData(
            active: new ChannelsData(ImmutableDictionary<string, GrpcChannel>.Empty),
            passive: new ChannelsData(ImmutableDictionary<string, GrpcChannel>.Empty));
    }

    public void Dispose()
    {
        Dispose(true);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }

        if (disposing)
        {
            lock (_updateLock)
            {
                foreach (var channel in _endpointsData.Active.Channels.Values)
                {
                    channel.Dispose();
                }

                foreach (var channel in _endpointsData.Passive.Channels.Values)
                {
                    channel.Dispose();
                }

                _endpointsData = new EndpointsData(
                    active: new ChannelsData(ImmutableDictionary<string, GrpcChannel>.Empty),
                    passive: new ChannelsData(ImmutableDictionary<string, GrpcChannel>.Empty));
            }
        }

        _disposed = true;
    }

    public void UpdateEndpoints(ListEndpointsResult listEndpointsResult)
    {
        var channelsToDispose = new List<GrpcChannel>();

        lock (_updateLock)
        {
            var freshEndpoints = new HashSet<string>();

            // Create channels for new endpoints
            var activeChannelsToAdd = new List<KeyValuePair<string, GrpcChannel>>();
            foreach (var endpoint in listEndpointsResult.Endpoints)
            {
                var endpointKey = MakeEndpointKey(endpoint);
                freshEndpoints.Add(endpointKey);

                if (_endpointsData.Active.Channels.ContainsKey(endpointKey))
                {
                    continue;
                }

                // Get from passive channels or create a new one
                GrpcChannel? channel;
                if (!_endpointsData.Passive.Channels.TryGetValue(endpointKey, out channel))
                {
                    channel = CreateChannel(endpointKey, _driverConfig);
                }

                activeChannelsToAdd.Add(KeyValuePair.Create(endpointKey, channel));
            }

            var activeChannelsToRemove = new List<string>();
            foreach (var (endpointKey, channel) in _endpointsData.Active.Channels)
            {
                if (!freshEndpoints.Contains(endpointKey))
                {
                    activeChannelsToRemove.Add(endpointKey);
                    channelsToDispose.Add(channel);
                }
            }

            var newActiveChannels = _endpointsData.Active.Channels
                .AddRange(activeChannelsToAdd)
                .RemoveRange(activeChannelsToRemove);

            foreach (var (endpointKey, channel) in _endpointsData.Passive.Channels)
            {
                if (!newActiveChannels.ContainsKey(endpointKey))
                {
                    channelsToDispose.Add(channel);
                }
            }

            _endpointsData = new EndpointsData(
                active: new ChannelsData(newActiveChannels),
                passive: new ChannelsData(ImmutableDictionary<string, GrpcChannel>.Empty));

            _logger.LogInformation($"Endpoints updated, active endpoints: {newActiveChannels.Count}");
        }

        foreach (var channel in channelsToDispose)
        {
            channel.Dispose();
        }

        channelsToDispose.Clear();
    }

    public double? PessimizeEndpoint(string endpoint)
    {
        lock (_updateLock)
        {
            GrpcChannel? channel;
            if (_endpointsData.Active.Channels.TryGetValue(endpoint, out channel))
            {
                var newActiveChannels = _endpointsData.Active.Channels
                    .Remove(endpoint);

                var newPassiveChannels = _endpointsData.Passive.Channels
                    .Add(endpoint, channel);

                _endpointsData = new EndpointsData(
                    active: new ChannelsData(newActiveChannels),
                    passive: new ChannelsData(newPassiveChannels));

                var activeEndpoints = _endpointsData.Active.Endpoints.Length;
                var passiveEndpoints = _endpointsData.Passive.Endpoints.Length;
                var passiveRatio = activeEndpoints + passiveEndpoints > 0
                    ? (double)passiveEndpoints / (activeEndpoints + passiveEndpoints)
                    : 1;

                _logger.LogInformation($"Endpoint pessimized: {endpoint}, passive ratio: {passiveRatio}");
                return passiveRatio;
            }

            return null;
        }
    }

    public (string, GrpcChannel) GetChannel(string? preferredEndpoint)
    {
        lock (_endpointsData)
        {
            var endpointsData = _endpointsData;

            GrpcChannel? channel;

            if (preferredEndpoint != null)
            {
                if (endpointsData.Active.Channels.TryGetValue(preferredEndpoint, out channel))
                {
                    return (preferredEndpoint, channel);
                }
            }

            if (TryGetEndpoint(endpointsData.Active, out var endpoint))
            {
                if (endpointsData.Active.Channels.TryGetValue(endpoint, out channel))
                {
                    return (endpoint, channel);
                }
            }

            if (TryGetEndpoint(endpointsData.Passive, out endpoint))
            {
                if (endpointsData.Passive.Channels.TryGetValue(endpoint, out channel))
                {
                    return (endpoint, channel);
                }
            }

            throw new NoEndpointsException();
        }
    }

    internal static GrpcChannel CreateChannel(
        string endpoint,
        DriverConfig config,
        ILoggerFactory? loggerFactory = null)
    {
        var channelOptions = new GrpcChannelOptions
        {
            LoggerFactory = loggerFactory
        };

        if (config.CustomServerCertificate != null)
        {
            var httpHandler = new SocketsHttpHandler();

            var customCertificate = DotNetUtilities.FromX509Certificate(config.CustomServerCertificate);

            httpHandler.SslOptions.RemoteCertificateValidationCallback =
                (_, certificate, _, sslPolicyErrors) =>
                {
                    if (sslPolicyErrors == SslPolicyErrors.None)
                    {
                        return true;
                    }

                    if (certificate is null)
                    {
                        return false;
                    }

                    try
                    {
                        var cert = DotNetUtilities.FromX509Certificate(certificate);
                        cert.Verify(customCertificate.GetPublicKey());
                    }
                    catch (Exception e)
                    {
                        var logger = loggerFactory.CreateLogger<ChannelsCache>();
                        logger.LogError($"Failed to verify remote certificate: {e}");
                        return false;
                    }

                    return true;
                };

            channelOptions.HttpHandler = httpHandler;
            channelOptions.DisposeHttpClient = true;
        }

        return GrpcChannel.ForAddress(endpoint, channelOptions);
    }

    private bool TryGetEndpoint(ChannelsData channelsData, [NotNullWhen(returnValue: true)] out string? endpoint)
    {
        if (channelsData.Endpoints.IsEmpty)
        {
            endpoint = null;
            return false;
        }

        endpoint = channelsData.Endpoints[_random.Next(0, channelsData.Endpoints.Length)];
        return true;
    }

    private static string MakeEndpointKey(EndpointInfo endpoint)
    {
        return $"{(endpoint.Ssl ? "https://" : "http://")}{endpoint.Address}:{endpoint.Port}";
    }

    public class NoEndpointsException : Exception
    {
    }

    private class ChannelsData
    {
        public readonly ImmutableDictionary<string, GrpcChannel> Channels;
        public readonly ImmutableArray<string> Endpoints;

        public ChannelsData(ImmutableDictionary<string, GrpcChannel> channels)
        {
            Channels = channels;
            Endpoints = channels.Keys.ToImmutableArray();
        }
    }

    private class EndpointsData
    {
        public readonly ChannelsData Active;
        public readonly ChannelsData Passive;

        public EndpointsData(ChannelsData active, ChannelsData passive)
        {
            Active = active;
            Passive = passive;
        }
    }
}
