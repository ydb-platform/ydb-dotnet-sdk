using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Org.BouncyCastle.Security;

namespace Ydb.Sdk.Pool;

internal class ChannelPool<T> : IAsyncDisposable where T : ChannelBase, IDisposable
{
    private readonly ConcurrentDictionary<string, Lazy<T>> _channels = new();

    private readonly ILogger<ChannelPool<T>> _logger;
    private readonly IChannelFactory<T> _channelFactory;

    public ChannelPool(ILogger<ChannelPool<T>> logger, IChannelFactory<T> channelFactory)
    {
        _logger = logger;
        _channelFactory = channelFactory;
    }

    public T GetChannel(string endpoint)
    {
        return _channels.GetOrAdd(endpoint, new Lazy<T>(() => _channelFactory.CreateChannel(endpoint))).Value;
    }

    public async ValueTask RemoveChannels(ImmutableArray<string> removedEndpoints)
    {
        var shutdownGrpcChannels = new List<T>();

        foreach (var endpoint in removedEndpoints)
        {
            if (_channels.TryRemove(endpoint, out var lazyGrpcChannel) && lazyGrpcChannel.IsValueCreated)
            {
                shutdownGrpcChannels.Add(lazyGrpcChannel.Value);
            }
        }

        await ShutdownChannels(shutdownGrpcChannels);
    }

    public async ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);

        await ShutdownChannels(_channels.Values
            .Where(lazyChannel => lazyChannel.IsValueCreated)
            .Select(lazyChannel => lazyChannel.Value)
            .ToImmutableArray()
        );
    }

    private async ValueTask ShutdownChannels(ICollection<T> channels)
    {
        foreach (var channel in channels)
        {
            try
            {
                await channel.ShutdownAsync();
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error occurred while shutting down the channel!");
            }

            channel.Dispose();
        }
    }
}

public interface IChannelFactory<out T> where T : ChannelBase, IDisposable
{
    T CreateChannel(string endpoint);
}

internal class GrpcChannelFactory : IChannelFactory<GrpcChannel>
{
    private readonly ILoggerFactory _loggerFactory;
    private readonly X509Certificate? _x509Certificate;
    private readonly ILogger<GrpcChannelFactory> _logger;

    internal GrpcChannelFactory(ILoggerFactory loggerFactory, DriverConfig config)
    {
        _loggerFactory = loggerFactory;
        _x509Certificate = config.CustomServerCertificate;
        _logger = loggerFactory.CreateLogger<GrpcChannelFactory>();
    }

    public GrpcChannel CreateChannel(string endpoint)
    {
        _logger.LogDebug("Initializing new gRPC channel for endpoint {Endpoint}", endpoint);

        var channelOptions = new GrpcChannelOptions
        {
            LoggerFactory = _loggerFactory
        };

        if (_x509Certificate == null)
        {
            return GrpcChannel.ForAddress(endpoint, channelOptions);
        }

        var customCertificate = DotNetUtilities.FromX509Certificate(_x509Certificate);

        var httpHandler = new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback =
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
                        _logger.LogError(e, "Failed to verify remote certificate!");

                        return false;
                    }

                    return true;
                }
        };
        httpHandler.Properties["__GrpcLoadBalancingDisabled"] = true;

        channelOptions.HttpHandler = httpHandler;
        channelOptions.DisposeHttpClient = true;

        return GrpcChannel.ForAddress(endpoint, channelOptions);
    }
}
