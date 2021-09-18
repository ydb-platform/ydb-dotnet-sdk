using System;
using Ydb.Sdk.Auth;

namespace Ydb.Sdk
{
    public class DriverConfig
    {
        public string Endpoint { get; }

        public string Database { get; }

        public ICredentialsProvider Credentials { get; }

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
            TimeSpan? defaultStreamingTransportTimeout = null)
        {
            Endpoint = endpoint;
            Database = database;
            Credentials = credentials ?? new AnonymousProvider();
            DefaultTransportTimeout = defaultTransportTimeout ?? TimeSpan.FromMinutes(1);
            DefaultStreamingTransportTimeout = defaultStreamingTransportTimeout ?? TimeSpan.FromMinutes(10);
        }
    }
}
