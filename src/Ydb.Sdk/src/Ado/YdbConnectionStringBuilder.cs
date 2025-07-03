using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Ydb.Sdk.Auth;
using Ydb.Sdk.Pool;
using Ydb.Sdk.Transport;

namespace Ydb.Sdk.Ado;

public sealed class YdbConnectionStringBuilder : DbConnectionStringBuilder
{
    public YdbConnectionStringBuilder()
    {
        InitDefaultValues();
    }

    public YdbConnectionStringBuilder(string connectionString)
    {
        InitDefaultValues();
        ConnectionString = connectionString;
    }

    // Init default connection string
    private void InitDefaultValues()
    {
        _host = YdbAdoDefaultSettings.Host;
        _port = YdbAdoDefaultSettings.Port;
        _database = YdbAdoDefaultSettings.Database;
        _maxSessionPool = SessionPoolDefaultSettings.MaxSessionPool;
        _useTls = YdbAdoDefaultSettings.UseTls;
        _connectTimeout = GrpcDefaultSettings.ConnectTimeoutSeconds;
        _keepAlivePingDelay = GrpcDefaultSettings.KeepAlivePingSeconds;
        _keepAlivePingTimeout = GrpcDefaultSettings.KeepAlivePingTimeoutSeconds;
        _enableMultipleHttp2Connections = GrpcDefaultSettings.EnableMultipleHttp2Connections;
        _maxSendMessageSize = GrpcDefaultSettings.MaxSendMessageSize;
        _maxReceiveMessageSize = GrpcDefaultSettings.MaxReceiveMessageSize;
        _disableDiscovery = GrpcDefaultSettings.DisableDiscovery;
        _createSessionTimeout = SessionPoolDefaultSettings.CreateSessionTimeoutSeconds;
    }

    public string Host
    {
        get => _host;
        set
        {
            _host = value;
            SaveValue(nameof(Host), value);
        }
    }

    private string _host = null!;

    public int Port
    {
        get => _port;
        set
        {
            if (value is <= 0 or > 65535)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "Invalid port: " + value);
            }

            _port = value;
            SaveValue(nameof(Port), value);
        }
    }

    private int _port;

    public string Database
    {
        get => _database;
        set
        {
            _database = value;
            SaveValue(nameof(Database), value);
        }
    }

    private string _database = null!;

    public string? User
    {
        get => _user;
        set
        {
            _user = value;
            SaveValue(nameof(User), value);
        }
    }

    private string? _user;

    public string? Password
    {
        get => _password;
        set
        {
            _password = value;
            SaveValue(nameof(Password), value);
        }
    }

    private string? _password;

    public int MaxSessionPool
    {
        get => _maxSessionPool;
        set
        {
            if (value <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "Invalid max session pool: " + value);
            }

            _maxSessionPool = value;
            SaveValue(nameof(MaxSessionPool), value);
        }
    }

    private int _maxSessionPool;

    public bool UseTls
    {
        get => _useTls;
        set
        {
            _useTls = value;
            SaveValue(nameof(UseTls), value);
        }
    }

    private bool _useTls;

    public string? RootCertificate
    {
        get => _rootCertificate;
        set
        {
            _rootCertificate = value;
            SaveValue(nameof(RootCertificate), value);

            UseTls = true;
        }
    }

    private string? _rootCertificate;

    public int ConnectTimeout
    {
        get => _connectTimeout;
        set
        {
            if (value < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "Invalid connect timeout: " + value);
            }

            _connectTimeout = value;
            SaveValue(nameof(ConnectTimeout), value);
        }
    }

    private int _connectTimeout;

    public int KeepAlivePingDelay
    {
        get => _keepAlivePingDelay;
        set
        {
            if (value < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "Invalid keep alive ping delay: " + value);
            }

            _keepAlivePingDelay = value;
            SaveValue(nameof(KeepAlivePingDelay), value);
        }
    }

    private int _keepAlivePingDelay;

    public int KeepAlivePingTimeout
    {
        get => _keepAlivePingTimeout;
        set
        {
            if (value < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value,
                    "Invalid keep alive ping timeout: " + value);
            }

            _keepAlivePingTimeout = value;
            SaveValue(nameof(KeepAlivePingTimeout), value);
        }
    }

    private int _keepAlivePingTimeout;

    public bool EnableMultipleHttp2Connections
    {
        get => _enableMultipleHttp2Connections;
        set
        {
            _enableMultipleHttp2Connections = value;
            SaveValue(nameof(EnableMultipleHttp2Connections), value);
        }
    }

    private bool _enableMultipleHttp2Connections;

    public int MaxSendMessageSize
    {
        get => _maxSendMessageSize;
        set
        {
            _maxSendMessageSize = value;
            SaveValue(nameof(MaxSendMessageSize), value);
        }
    }

    private int _maxSendMessageSize;

    public int MaxReceiveMessageSize
    {
        get => _maxReceiveMessageSize;
        set
        {
            _maxReceiveMessageSize = value;
            SaveValue(nameof(MaxReceiveMessageSize), value);
        }
    }

    private int _maxReceiveMessageSize;

    public bool DisableServerBalancer
    {  
        get => _disableServerBalancer;
        set
        {
            _disableServerBalancer = value;
            SaveValue(nameof(DisableServerBalancer), value);
        }
    }

    private bool _disableServerBalancer;

    public bool DisableDiscovery
    {
        get => _disableDiscovery;
        set
        {
            _disableDiscovery = value;
            SaveValue(nameof(DisableDiscovery), value);
        }
    }

    private bool _disableDiscovery;

    public int CreateSessionTimeout
    {
        get => _createSessionTimeout;
        set
        {
            if (value < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value,
                    "Invalid create session timeout: " + value);
            }

            _createSessionTimeout = value;
            SaveValue(nameof(CreateSessionTimeout), value);
        }
    }

    private int _createSessionTimeout;

    public ILoggerFactory? LoggerFactory { get; init; }

    public ICredentialsProvider? CredentialsProvider { get; init; }

    public X509Certificate2Collection? ServerCertificates { get; init; }

    private void SaveValue(string propertyName, object? value)
    {
        if (value == null)
        {
            Remove(propertyName);
        }
        else
        {
            base[propertyName] = value;
        }
    }

    public override object this[string keyword]
    {
        get => base[keyword];
#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
        [param: AllowNull]
        set
#pragma warning restore CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
        {
            if (!YdbConnectionOption.KeyToOption.TryGetValue(keyword, out var option))
            {
                throw new ArgumentException("Key doesn't support: " + keyword);
            }

            // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
            if (value == null)
            {
                Remove(keyword);
                return;
            }

            option.UpdateConnectionBuilder(this, keyword, value);
        }
    }

    private string Endpoint => $"{(UseTls ? "grpcs" : "grpc")}://{Host}:{Port}";

    internal async Task<IDriver> BuildDriver()
    {
        var cert = RootCertificate != null ? X509Certificate.CreateFromCertFile(RootCertificate) : null;
        var driverConfig = new DriverConfig(
            endpoint: Endpoint,
            database: Database,
            credentials: CredentialsProvider,
            customServerCertificate: cert,
            customServerCertificates: ServerCertificates
        )
        {
            ConnectTimeout = ConnectTimeout == 0
                ? Timeout.InfiniteTimeSpan
                : TimeSpan.FromSeconds(ConnectTimeout),
            KeepAlivePingDelay = KeepAlivePingDelay == 0
                ? Timeout.InfiniteTimeSpan
                : TimeSpan.FromSeconds(KeepAlivePingDelay),
            KeepAlivePingTimeout = KeepAlivePingTimeout == 0
                ? Timeout.InfiniteTimeSpan
                : TimeSpan.FromSeconds(KeepAlivePingTimeout),
            User = User,
            Password = Password,
            EnableMultipleHttp2Connections = EnableMultipleHttp2Connections,
            MaxSendMessageSize = MaxSendMessageSize,
            MaxReceiveMessageSize = MaxReceiveMessageSize,
            DisableServerBalancer = DisableServerBalancer
        };
        var loggerFactory = LoggerFactory ?? NullLoggerFactory.Instance;

        return DisableDiscovery
            ? new DirectGrpcChannelDriver(driverConfig, loggerFactory)
            : await Driver.CreateInitialized(driverConfig, loggerFactory);
    }

    public override void Clear()
    {
        base.Clear();
        InitDefaultValues();
    }

    private abstract class YdbConnectionOption
    {
        private static readonly Func<string, object, int> IntExtractor = (key, value) =>
        {
            return value switch
            {
                int intValue => intValue,
                string strValue => int.TryParse(strValue.Trim(), out var result)
                    ? result
                    : throw new ArgumentException("Invalid value for integer conversion"),
                _ => UnexpectedArgumentException<int>(key, value)
            };
        };

        private static readonly Func<string, object, string> StringExtractor = (key, value) =>
        {
            return value switch
            {
                string strValue => strValue,
                _ => UnexpectedArgumentException<string>(key, value)
            };
        };

        private static readonly Func<string, object, bool> BoolExtractor = (key, value) =>
        {
            return value switch
            {
                bool boolValue => boolValue,
                string strValue => strValue.ToLowerInvariant() switch
                {
                    "on" or "true" or "1" => true,
                    "off" or "false" or "0" => false,
                    _ => throw new ArgumentException("Invalid value for boolean conversion")
                },
                _ => UnexpectedArgumentException<bool>(key, value)
            };
        };

        [DoesNotReturn]
        private static T UnexpectedArgumentException<T>(string key, object value) =>
            throw new ArgumentException($"Expected type {typeof(T)} for key {key}, but actual {value.GetType()}");

        public static readonly Dictionary<string, YdbConnectionOption> KeyToOption = new();

        static YdbConnectionOption()
        {
            AddOption(new YdbConnectionOption<int>(IntExtractor, (builder, port) => builder.Port = port), "Port");
            AddOption(new YdbConnectionOption<string>(StringExtractor, (builder, host) => builder.Host = host), "Host");
            AddOption(new YdbConnectionOption<string>(StringExtractor,
                (builder, database) => builder.Database = database), "Database");
            AddOption(new YdbConnectionOption<string>(StringExtractor,
                (builder, user) => builder.User = user), "User", "Username", "UserId", "User Id");
            AddOption(new YdbConnectionOption<string>(StringExtractor,
                (builder, password) => builder.Password = password), "Password", "PWD", "PSW");
            AddOption(new YdbConnectionOption<int>(IntExtractor,
                    (builder, maxSessionPool) => builder.MaxSessionPool = maxSessionPool),
                "MaxSessionPool", "Max Session Pool", "Maximum Pool Size", "Max Pool Size", "MaximumPoolSize");
            AddOption(new YdbConnectionOption<bool>(BoolExtractor, (builder, useTls) => builder.UseTls = useTls),
                "UseTls", "Use Tls");
            AddOption(new YdbConnectionOption<string>(StringExtractor,
                    (builder, rootCertificate) => builder.RootCertificate = rootCertificate),
                "RootCertificate", "Root Certificate");
            AddOption(new YdbConnectionOption<int>(IntExtractor,
                    (builder, connectTimeout) => builder.ConnectTimeout = connectTimeout),
                "ConnectTimeout", "Connect Timeout");
            AddOption(new YdbConnectionOption<int>(IntExtractor,
                    (builder, keepAlivePingDelay) => builder.KeepAlivePingDelay = keepAlivePingDelay),
                "KeepAlivePingDelay", "Keep Alive Ping Delay");
            AddOption(new YdbConnectionOption<int>(IntExtractor,
                    (builder, keepAlivePingTimeout) => builder.KeepAlivePingTimeout = keepAlivePingTimeout),
                "KeepAlivePingTimeout", "Keep Alive Ping Timeout");
            AddOption(new YdbConnectionOption<bool>(BoolExtractor, (builder, enableMultipleHttp2Connections) =>
                    builder.EnableMultipleHttp2Connections = enableMultipleHttp2Connections),
                "EnableMultipleHttp2Connections", "Enable Multiple Http2 Connections");
            AddOption(new YdbConnectionOption<int>(IntExtractor, (builder, maxSendMessageSize) =>
                builder.MaxSendMessageSize = maxSendMessageSize), "MaxSendMessageSize", "Max Send Message Size");
            AddOption(new YdbConnectionOption<int>(IntExtractor, (builder, maxReceiveMessageSize) =>
                    builder.MaxReceiveMessageSize = maxReceiveMessageSize),
                "MaxReceiveMessageSize", "Max Receive Message Size");
            AddOption(new YdbConnectionOption<bool>(BoolExtractor, (builder, disableDiscovery) =>
                builder.DisableDiscovery = disableDiscovery), "DisableDiscovery", "Disable Discovery");
            AddOption(new YdbConnectionOption<int>(IntExtractor,
                    (builder, createSessionTimeout) => builder.CreateSessionTimeout = createSessionTimeout),
                "CreateSessionTimeout", "Create Session Timeout");
            AddOption(new YdbConnectionOption<bool>(BoolExtractor, (builder, disableServerBalancer) =>
                    builder.DisableServerBalancer = disableServerBalancer),
                "DisableServerBalancer", "Disable Server Balancer");
        }

        private static void AddOption(YdbConnectionOption option, params string[] keys)
        {
            foreach (var key in keys)
            {
                KeyToOption.Add(key.Trim(), option);
                KeyToOption.Add(key.ToLower(), option);
            }
        }

        public abstract void UpdateConnectionBuilder(YdbConnectionStringBuilder builder, string key, object value);
    }

    private class YdbConnectionOption<T> : YdbConnectionOption
    {
        private Func<string, object, T> Extract { get; }
        private Action<YdbConnectionStringBuilder, T> ConnectionBuilderSetter { get; }

        internal YdbConnectionOption(Func<string, object, T> extract,
            Action<YdbConnectionStringBuilder, T> connectionBuilderSetter)
        {
            Extract = extract;
            ConnectionBuilderSetter = connectionBuilderSetter;
        }

        public override void UpdateConnectionBuilder(YdbConnectionStringBuilder builder, string key, object value)
        {
            var extractedValue = Extract(key, value);
            ConnectionBuilderSetter(builder, extractedValue);
        }
    }
}
