using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;

namespace Ydb.Sdk
{
    internal class ChannelsCache : IDisposable
    {
        private readonly ILogger _logger;
        private readonly object _updateLock = new object();
        private EndpointsData _endpointsData;
        private bool _disposed = false;

        private readonly Random _random = new Random();

        public ChannelsCache(ILoggerFactory loggerFactory)
        {
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

        public void UpdateEndpoints(Discovery.ListEndpointsResult listEndpointsResult)
        {
            var channelsToDispose = new List<GrpcChannel>();

            lock (_updateLock)
            {
                var freshEndpoints = new HashSet<string>();

                // Create channels for new endpoints
                var activeChannelsToAdd = new List<KeyValuePair<string, GrpcChannel>>();
                foreach (Discovery.EndpointInfo endpoint in listEndpointsResult.Endpoints)
                {
                    string endpointKey = MakeEndpointKey(endpoint);
                    freshEndpoints.Add(endpointKey);

                    if (_endpointsData.Active.Channels.ContainsKey(endpointKey))
                    {
                        continue;
                    }

                    // Get from passive channels or create a new one
                    GrpcChannel? channel;
                    if (!_endpointsData.Passive.Channels.TryGetValue(endpointKey, out channel))
                    {
                        channel = GrpcChannel.ForAddress(endpointKey);
                    }

                    activeChannelsToAdd.Add(KeyValuePair.Create(endpointKey, channel));
                }

                var activeChannelsToRemove = new List<string>();
                foreach ((string endpointKey, GrpcChannel channel) in _endpointsData.Active.Channels)
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

                foreach ((string endpointKey, GrpcChannel channel) in _endpointsData.Passive.Channels)
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

            foreach (GrpcChannel channel in channelsToDispose)
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

                    int activeEndpoints = _endpointsData.Active.Endpoints.Length;
                    int passiveEndpoints = _endpointsData.Passive.Endpoints.Length;
                    double passiveRatio = activeEndpoints + passiveEndpoints > 0
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
            var endpointsData = _endpointsData;

            string? endpoint;
            GrpcChannel? channel;

            if (preferredEndpoint != null)
            {
                if (endpointsData.Active.Channels.TryGetValue(preferredEndpoint, out channel))
                {
                    return (preferredEndpoint, channel);
                }
            }

            if (TryGetEndpoint(endpointsData.Active, out endpoint)) {
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

        private static string MakeEndpointKey(Discovery.EndpointInfo endpoint)
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
}
