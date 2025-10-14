using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Ydb.Sdk.Auth;
using Ydb.Sdk.Transport;

namespace Ydb.Sdk.Ado;

/// <summary>
/// Provides a simple way to create and manage the contents of connection strings used by
/// the <see cref="YdbConnection"/> class.
/// </summary>
/// <remarks>
/// YdbConnectionStringBuilder provides strongly-typed properties for building YDB connection strings.
/// It supports all standard ADO.NET connection string parameters plus YDB-specific options.
/// 
/// <para>
/// For more information about YDB, see:
/// <see href="https://ydb.tech/docs">YDB Documentation</see>.
/// </para>
/// </remarks>
public sealed class YdbConnectionStringBuilder : DbConnectionStringBuilder
{
    /// <summary>
    /// Initializes a new instance of the <see cref="YdbConnectionStringBuilder"/> class.
    /// </summary>
    public YdbConnectionStringBuilder()
    {
        InitDefaultValues();
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbConnectionStringBuilder"/> class
    /// with the specified connection string.
    /// </summary>
    /// <param name="connectionString">
    /// The connection string to parse and use as the basis for the internal connection string.
    /// </param>
    public YdbConnectionStringBuilder(string connectionString)
    {
        InitDefaultValues();
        ConnectionString = connectionString;
    }

    // Init default connection string
    private void InitDefaultValues()
    {
        _host = "localhost";
        _port = 2136;
        _database = "/local";
        _minPoolSize = 0;
        _maxPoolSize = 100;
        _createSessionTimeout = 5;
        _sessionIdleTimeout = 300;
        _useTls = false;
        _connectTimeout = GrpcDefaultSettings.ConnectTimeoutSeconds;
        _keepAlivePingDelay = GrpcDefaultSettings.KeepAlivePingSeconds;
        _keepAlivePingTimeout = GrpcDefaultSettings.KeepAlivePingTimeoutSeconds;
        _enableMultipleHttp2Connections = GrpcDefaultSettings.EnableMultipleHttp2Connections;
        _maxSendMessageSize = GrpcDefaultSettings.MaxSendMessageSize;
        _maxReceiveMessageSize = GrpcDefaultSettings.MaxReceiveMessageSize;
        _disableDiscovery = GrpcDefaultSettings.DisableDiscovery;
        _disableServerBalancer = false;
        _enableImplicitSession = false;
    }

    /// <summary>
    /// Gets or sets the host name or IP address of the YDB server.
    /// </summary>
    /// <remarks>
    /// Specifies the hostname or IP address where the YDB server is running.
    /// <para>Default value: localhost.</para>
    /// </remarks>
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

    /// <summary>
    /// Gets or sets the port number of the YDB server.
    /// </summary>
    /// <remarks>
    /// Specifies the port number where the YDB server is listening.
    /// Must be between 1 and 65535.
    /// <para>Default value: 2136.</para>
    /// </remarks>
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

    /// <summary>
    /// Gets or sets the database path.
    /// </summary>
    /// <remarks>
    /// Specifies the path to the YDB database to connect to.
    /// <para>Default value: /local.</para>
    /// </remarks>
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

    /// <summary>
    /// Gets or sets the username for authentication.
    /// </summary>
    /// <remarks>
    /// Specifies the username used for authenticating with the YDB server.
    /// If not specified, authentication using a username and password is disabled.
    /// <para>Default value: null.</para>
    /// </remarks>
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

    /// <summary>
    /// Gets or sets the password for authentication.
    /// </summary>
    /// <remarks>
    /// Specifies the password used for authentication with the YDB server.
    /// If not specified, a password is not used.
    /// <para>Default value: null.</para>
    /// </remarks>
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

    /// <summary>
    /// Gets or sets the maximum number of sessions in the pool.
    /// </summary>
    /// <remarks>
    /// Specifies the maximum number of sessions that can be created and maintained
    /// in the session pool. Must be greater than 0.
    /// <para>Default value: 100.</para>
    /// </remarks>
    public int MaxPoolSize
    {
        get => _maxPoolSize;
        set
        {
            if (value <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "Invalid max session pool: " + value);
            }

            _maxPoolSize = value;
            SaveValue(nameof(MaxPoolSize), value);
        }
    }

    private int _maxPoolSize;

    /// <summary>
    /// Gets or sets the minimum number of sessions in the pool.
    /// </summary>
    /// <remarks>
    /// Specifies the minimum number of sessions to maintain in the session pool.
    /// Must be greater than or equal to 0.
    /// <para>Default value: 0.</para>
    /// </remarks>
    public int MinPoolSize
    {
        get => _minPoolSize;
        set
        {
            if (value < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "Invalid min session pool: " + value);
            }

            _minPoolSize = value;
            SaveValue(nameof(MinPoolSize), value);
        }
    }

    private int _minPoolSize;

    /// <summary>
    /// Gets or sets the session idle timeout in seconds.
    /// </summary>
    /// <remarks>
    /// Specifies how long a session can remain idle before being closed.
    /// Must be greater than or equal to 0.
    /// <para>Default value: 300 seconds (5 minutes).</para>
    /// </remarks>
    public int SessionIdleTimeout
    {
        get => _sessionIdleTimeout;
        set
        {
            if (value < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "Invalid session idle timeout: " + value);
            }

            _sessionIdleTimeout = value;
            SaveValue(nameof(SessionIdleTimeout), value);
        }
    }

    private int _sessionIdleTimeout;

    /// <summary>
    /// Gets or sets a value indicating whether to use TLS encryption.
    /// </summary>
    /// <remarks>
    /// When true, the connection uses TLS encryption (grpcs://).
    /// When false, the connection uses plain text (grpc://).
    /// <para>Default value: false.</para>
    /// </remarks>
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

    /// <summary>
    /// Gets or sets the path to the root certificate file.
    /// </summary>
    /// <remarks>
    /// Specifies the path to a PEM-encoded root certificate file for TLS verification.
    /// Setting this property automatically enables TLS (UseTls = true).
    /// <para>Default value: null.</para>
    /// </remarks>
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

    /// <summary>
    /// Gets or sets the connection timeout in seconds.
    /// </summary>
    /// <remarks>
    /// Specifies the maximum time to wait when establishing a connection to the server.
    /// Must be greater than or equal to 0. Set to 0 for infinite timeout.
    /// <para>Default value: 10 seconds.</para>
    /// </remarks>
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

    /// <summary>
    /// Gets or sets the keep-alive ping delay in seconds.
    /// </summary>
    /// <remarks>
    /// Specifies the interval between keep-alive ping messages to detect broken connections.
    /// Must be greater than or equal to 0. Set to 0 to disable keep-alive pings.
    /// <para>Default value: 10 seconds.</para>
    /// </remarks>
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

    /// <summary>
    /// Gets or sets the keep-alive ping timeout in seconds.
    /// </summary>
    /// <remarks>
    /// Specifies the maximum time to wait for a keep-alive ping response before
    /// considering the connection broken. Must be greater than or equal to 0.
    /// Set to 0 for infinite timeout.
    /// <para>Default value: 5 seconds.</para>
    /// </remarks>
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

    /// <summary>
    /// Gets or sets a value indicating whether to enable multiple HTTP/2 connections.
    /// </summary>
    /// <remarks>
    /// When true, enables automatic scaling of HTTP/2 connections within a single gRPC channel
    /// to one node of the cluster. This is rarely needed but can improve performance for
    /// high-load scenarios with a single node.
    /// When false, uses a single HTTP/2 connection per node.
    /// <para>Default value: false.</para>
    /// </remarks>
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

    /// <summary>
    /// Gets or sets the maximum size for outgoing messages in bytes.
    /// </summary>
    /// <remarks>
    /// Specifies the maximum size of messages that can be sent to the server.
    /// Note: server-side limit is 64 MB. Exceeding this limit may result in
    /// "resource exhausted" errors or unpredictable behavior.
    /// <para>Default value: 67108864 bytes (64 MB).</para>
    /// </remarks>
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

    /// <summary>
    /// Gets or sets the maximum size for incoming messages in bytes.
    /// </summary>
    /// <remarks>
    /// Specifies the maximum size of messages that can be received from the server.
    /// <para>Default value: 67108864 bytes (64 MB).</para>
    /// </remarks>
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

    /// <summary>
    /// Gets or sets a value indicating whether to disable server load balancing.
    /// </summary>
    /// <remarks>
    /// When true, disables server load balancing and uses direct connections.
    /// When false, enables server load balancing for better performance.
    /// <para>Default value: false.</para>
    /// </remarks>
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

    /// <summary>
    /// Gets or sets a value indicating whether to disable service discovery.
    /// </summary>
    /// <remarks>
    /// When true, disables automatic service discovery and uses direct gRPC connections.
    /// When false, enables service discovery for automatic endpoint resolution.
    /// <para>Default value: false.</para>
    /// </remarks>
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

    /// <summary>
    /// Gets or sets the session creation timeout in seconds.
    /// </summary>
    /// <remarks>
    /// Specifies the maximum time to wait when creating a new session.
    /// Must be greater than or equal to 0. Set to 0 for infinite timeout.
    /// <para>Default value: 5 seconds.</para>
    /// </remarks>
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

    /// <summary>
    /// Gets or sets a value indicating whether to enable implicit session management.
    /// </summary>
    /// <remarks>
    /// When true, implicit session management is enabled: the server creates and destroys
    /// sessions automatically for each operation. In this mode, the Session Pool is not created,
    /// and sessions are not stored on the client side. Interactive client transactions are
    /// not supported in this mode.
    /// When false, the standard YDB sessions for tables (YDB has topics, coordination service, etc.) are used.
    /// <para>Default value: false.</para>
    /// </remarks>
    public bool EnableImplicitSession
    {
        get => _enableImplicitSession;
        set
        {
            _enableImplicitSession = value;
            SaveValue(nameof(EnableImplicitSession), value);
        }
    }

    private bool _enableImplicitSession;

    /// <summary>
    /// Gets or sets the logger factory for logging operations.
    /// </summary>
    /// <remarks>
    /// Specifies the logger factory used for creating loggers throughout the SDK.
    /// <para>Default value: NullLoggerFactory.Instance (no logging).</para>
    /// </remarks>
    public ILoggerFactory LoggerFactory { get; init; } = NullLoggerFactory.Instance;

    /// <summary>
    /// Gets or sets the credentials provider for authentication.
    /// </summary>
    /// <remarks>
    /// Specifies the credentials provider used for authenticating with the YDB server.
    /// If not provided, authentication is not used.
    /// <para>Default value: null.</para>
    /// </remarks>
    public ICredentialsProvider? CredentialsProvider { get; init; }

    /// <summary>
    /// Gets or sets the collection of server certificates for TLS verification.
    /// </summary>
    /// <remarks>
    /// Specifies additional server certificates to trust for TLS verification.
    /// If not provided, the system certificate store is used.
    /// <para>Default value: null.</para>
    /// </remarks>
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

    internal string GrpcConnectionString =>
        $"UseTls={UseTls};Host={Host};Port={Port};Database={Database};User={User};Password={Password};" +
        $"ConnectTimeout={ConnectTimeout};KeepAlivePingDelay={KeepAlivePingDelay};KeepAlivePingTimeout={KeepAlivePingTimeout};" +
        $"EnableMultipleHttp2Connections={EnableMultipleHttp2Connections};MaxSendMessageSize={MaxSendMessageSize};" +
        $"MaxReceiveMessageSize={MaxReceiveMessageSize};DisableDiscovery={DisableDiscovery}";

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
            MaxReceiveMessageSize = MaxReceiveMessageSize
        };

        return DisableDiscovery
            ? new DirectGrpcChannelDriver(driverConfig, LoggerFactory)
            : await Driver.CreateInitialized(driverConfig, LoggerFactory);
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
                    (builder, maxPoolSize) => builder.MaxPoolSize = maxPoolSize),
                "Maximum Pool Size", "MaximumPoolSize", "Max Pool Size", "MaxPoolSize");
            AddOption(new YdbConnectionOption<int>(IntExtractor,
                    (builder, minPoolSize) => builder.MinPoolSize = minPoolSize),
                "Minimum Pool Size", "MinimumPoolSize", "Min Pool Size", "MinPoolSize");
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
            AddOption(new YdbConnectionOption<int>(IntExtractor,
                    (builder, sessionIdleTimeout) => builder.SessionIdleTimeout = sessionIdleTimeout),
                "SessionIdleTimeout", "Session Idle Timeout");
            AddOption(new YdbConnectionOption<bool>(BoolExtractor,
                    (builder, disableServerBalancer) => builder.DisableServerBalancer = disableServerBalancer),
                "DisableServerBalancer", "Disable Server Balancer");
            AddOption(new YdbConnectionOption<bool>(BoolExtractor,
                    (builder, enableImplicitSession) => builder.EnableImplicitSession = enableImplicitSession),
                "EnableImplicitSession", "Enable Implicit Session");
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
