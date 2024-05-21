using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Net.Security;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Org.BouncyCastle.Security;
using X509Certificate = System.Security.Cryptography.X509Certificates.X509Certificate;

namespace Ydb.Sdk.Pool;

public class GrpcChannelPool<T> : IAsyncDisposable where T : ChannelBase, IDisposable
{
    private readonly ConcurrentDictionary<string, T> _channels = new();

    private readonly ILogger<GrpcChannelPool<T>> _logger;
    private readonly IGrpcChannelFactory<T> _channelFactory;

    public GrpcChannelPool(ILogger<GrpcChannelPool<T>> logger, IGrpcChannelFactory<T> channelFactory)
    {
        _logger = logger;
        _channelFactory = channelFactory;
    }

    public T GetChannel(string endpoint)
    {
        if (_channels.TryGetValue(endpoint, out var channel))
        {
            return channel;
        }

        _logger.LogDebug("Grpc channel {Endpoint} was not found in pool, so it starts creating it ...", endpoint);
        return _channels.GetOrAdd(endpoint, _channelFactory.CreateChannel(endpoint));
    }

    public async ValueTask RemoveChannels(ImmutableArray<string> removedEndpoints)
    {
        var shutdownGrpcChannels = new List<T>();

        foreach (var endpoint in removedEndpoints)
        {
            if (_channels.TryRemove(endpoint, out var grpcChannel))
            {
                shutdownGrpcChannels.Add(grpcChannel);
            }
        }

        await ShutdownChannels(shutdownGrpcChannels);
    }

    public async ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);

        await ShutdownChannels(_channels.Values);
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

public interface IGrpcChannelFactory<out T> where T : ChannelBase, IDisposable
{
    T CreateChannel(string endpoint);
}

internal class GrpcChannelFactory : IGrpcChannelFactory<GrpcChannel>
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
        var channelOptions = new GrpcChannelOptions
        {
            LoggerFactory = _loggerFactory
        };

        if (_x509Certificate == null)
        {
            return GrpcChannel.ForAddress(endpoint, channelOptions);
        }

        var httpHandler = new SocketsHttpHandler();

        var customCertificate = DotNetUtilities.FromX509Certificate(_x509Certificate);

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
                    _logger.LogError(e, "Failed to verify remote certificate!");

                    return false;
                }

                return true;
            };

        channelOptions.HttpHandler = httpHandler;
        channelOptions.DisposeHttpClient = true;

        return GrpcChannel.ForAddress(endpoint, channelOptions);
    }
}
