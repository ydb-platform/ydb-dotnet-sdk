using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Ydb.Sdk.Ado;

public sealed class YdbConnectionStringBuilder : DbConnectionStringBuilder
{
    public YdbConnectionStringBuilder()
    {
        InitDefaultValues();
    }

    internal YdbConnectionStringBuilder(string connectionString)
    {
        InitDefaultValues();
        ConnectionString = connectionString;
    }

    // Init default connection string
    private void InitDefaultValues()
    {
        Host = "localhost";
        Port = 2136;
        Database = "/local";
        MaxSessionPool = 100;
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
                int valueInt => valueInt,
                string valueStr when int.TryParse(valueStr.Trim(), out var result) => result,
                _ => UnexpectedArgumentException<int>(key, value)
            };
        };

        private static readonly Func<string, object, string> StringExtractor = (key, value) =>
        {
            return value switch
            {
                string valueStr => valueStr,
                _ => UnexpectedArgumentException<string>(key, value)
            };
        };

        [DoesNotReturn]
        private static T UnexpectedArgumentException<T>(string key, object value)
        {
            throw new ArgumentException($"Expected type {typeof(T)} for key {key}, but actual {value.GetType()}");
        }

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
