using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Auth;

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
        _host = "localhost";
        _port = 2136;
        _database = "/local";
        _maxSessionPool = 100;
        _useTls = false;
        _keepAlivePingDelay = SocketHttpHandlerDefaults.DefaultKeepAlivePingSeconds;
        _keepAlivePingTimeout = SocketHttpHandlerDefaults.DefaultKeepAlivePingTimeoutSeconds;
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

    public int KeepAlivePingDelay
    {
        get => _keepAlivePingDelay;
        set
        {
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
            _keepAlivePingTimeout = value;
            SaveValue(nameof(KeepAlivePingTimeout), value);
        }
    }

    private int _keepAlivePingTimeout;

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

    internal Task<Driver> BuildDriver()
    {
        var credentialsProvider = CredentialsProvider ??
                                  (User != null ? new StaticCredentialsProvider(User, Password) : null);
        var cert = RootCertificate != null ? X509Certificate.CreateFromCertFile(RootCertificate) : null;

        return Driver.CreateInitialized(new DriverConfig(
            endpoint: Endpoint,
            database: Database,
            credentials: credentialsProvider,
            customServerCertificate: cert,
            customServerCertificates: ServerCertificates
        )
        {
            KeepAlivePingDelay = TimeSpan.FromSeconds(KeepAlivePingDelay),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(KeepAlivePingTimeout)
        }, LoggerFactory);
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
                string strValue => strValue switch
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
            AddOption(
                new YdbConnectionOption<string>(StringExtractor,
                    (builder, rootCertificate) => builder.RootCertificate = rootCertificate),
                "RootCertificate", "Root Certificate");
            AddOption(new YdbConnectionOption<int>(IntExtractor,
                    (builder, keepAlivePingDelay) => builder.KeepAlivePingDelay = keepAlivePingDelay),
                "KeepAlivePingDelay", "Keep Alive Ping Delay");
            AddOption(new YdbConnectionOption<int>(IntExtractor,
                    (builder, keepAlivePingTimeout) => builder.KeepAlivePingTimeout = keepAlivePingTimeout),
                "KeepAlivePingTimeout", "Keep Alive Ping Timeout");
        }

        private static void AddOption(YdbConnectionOption option, params string[] keys)
        {
            foreach (var key in keys)
            {
                KeyToOption.Add(key, option);
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
