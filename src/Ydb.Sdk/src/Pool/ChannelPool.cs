using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;

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

    public T GetChannel(string endpoint) =>
        _channels.GetOrAdd(endpoint, new Lazy<T>(() => _channelFactory.CreateChannel(endpoint))).Value;

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
    private readonly ILogger<GrpcChannelFactory> _logger;
    private readonly X509Certificate2Collection _x509Certificate2Collection;

    internal GrpcChannelFactory(ILoggerFactory loggerFactory, DriverConfig config)
    {
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<GrpcChannelFactory>();
        _x509Certificate2Collection = config.CustomServerCertificates;
    }

    public GrpcChannel CreateChannel(string endpoint)
    {
        _logger.LogDebug("Initializing new gRPC channel for endpoint {Endpoint}", endpoint);

        var channelOptions = new GrpcChannelOptions
        {
            LoggerFactory = _loggerFactory
        };

        var httpHandler = new SocketsHttpHandler();

        // https://github.com/grpc/grpc-dotnet/issues/2312#issuecomment-1790661801
        httpHandler.Properties["__GrpcLoadBalancingDisabled"] = true;

        channelOptions.HttpHandler = httpHandler;
        channelOptions.DisposeHttpClient = true;

        if (_x509Certificate2Collection.Count == 0)
        {
            return GrpcChannel.ForAddress(endpoint, channelOptions);
        }

        httpHandler.SslOptions.RemoteCertificateValidationCallback +=
            (_, certificate, chain, sslPolicyErrors) =>
            {
                if (sslPolicyErrors == SslPolicyErrors.None)
                {
                    return true;
                }

                if (certificate is null || chain is null)
                {
                    return false;
                }

                try
                {
                    chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
                    chain.ChainPolicy.ExtraStore.AddRange(_x509Certificate2Collection);

                    return chain.Build(new X509Certificate2(certificate)) && chain.ChainElements.Any(chainElement =>
                        _x509Certificate2Collection.Any(trustedCert =>
                            chainElement.Certificate.Thumbprint == trustedCert.Thumbprint));
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Failed to verify remote certificate!");

                    return false;
                }
            };

        return GrpcChannel.ForAddress(endpoint, channelOptions);
    }
}
