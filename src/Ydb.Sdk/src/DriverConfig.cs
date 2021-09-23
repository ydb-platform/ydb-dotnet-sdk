using System;
using System.Security.Cryptography.X509Certificates;
using Ydb.Sdk.Auth;

namespace Ydb.Sdk
{
    public class DriverConfig
    {
        public string Endpoint { get; }

        public string Database { get; }

        public ICredentialsProvider Credentials { get; }

        public X509Certificate? CustomServerCertificate { get; }

        public TimeSpan DefaultTransportTimeout { get; }

        public TimeSpan DefaultStreamingTransportTimeout { get; }

        internal TimeSpan EndpointDiscoveryInterval = TimeSpan.FromMinutes(1);
        internal double PessimizedEndpointRatioTreshold = 0.5;
        internal TimeSpan EndpointDiscoveryTimeout = TimeSpan.FromSeconds(10);

        public DriverConfig(
            string endpoint,
            string database,
            ICredentialsProvider? credentials = null,
            TimeSpan? defaultTransportTimeout = null,
            TimeSpan? defaultStreamingTransportTimeout = null,
            X509Certificate? customServerCertificate = null)
        {
            Endpoint = FormatEndpoint(endpoint);
            Database = database;
            Credentials = credentials ?? new AnonymousProvider();
            DefaultTransportTimeout = defaultTransportTimeout ?? TimeSpan.FromMinutes(1);
            DefaultStreamingTransportTimeout = defaultStreamingTransportTimeout ?? TimeSpan.FromMinutes(10);
            CustomServerCertificate = customServerCertificate;
        }

        private static string FormatEndpoint(string endpoint)
        {
            endpoint = endpoint.ToLower().Trim();

            if (endpoint.StartsWith("http://") || endpoint.StartsWith("https://"))
            {
                return endpoint;
            }

            if (endpoint.StartsWith("grpc://")) {
                var builder = new UriBuilder(endpoint) { Scheme = Uri.UriSchemeHttp };
                return builder.Uri.ToString();
            }

            if (endpoint.StartsWith("grpcs://")) {
                var builder = new UriBuilder(endpoint) { Scheme = Uri.UriSchemeHttps };
                return builder.Uri.ToString();
            }

            return $"https://{endpoint}";
        }
    }
}
