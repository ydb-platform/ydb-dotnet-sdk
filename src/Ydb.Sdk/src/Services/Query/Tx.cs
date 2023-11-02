using Ydb.Query;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query;

public interface ITxModeSettings
{
}

public class TxModeSerializableSettings : ITxModeSettings
{
}

public class TxModeOnlineSettings : ITxModeSettings
{
    public TxModeOnlineSettings(bool allowInconsistentReads = false)
    {
        AllowInconsistentReads = allowInconsistentReads;
    }

    public bool AllowInconsistentReads { get; }
}

public class TxModeStaleSettings : ITxModeSettings
{
}

public class TxModeSnapshotSettings : ITxModeSettings
{
}

public class Tx
{
    public string? TxId
    {
        get => _proto.TxId;
        set
        {
            _proto.TxId = value;
            if (!string.IsNullOrEmpty(value))
                _proto.BeginTx = null;
        }
    }

    private TransactionControl _proto;

    public Tx()
    {
        _proto = new TransactionControl();
    }

    internal Tx(TransactionControl proto)
    {
        _proto = proto;
    }

    internal TransactionControl ToProto()
    {
        return _proto.Clone();
    }

    public static Tx Begin(ITxModeSettings? txModeSettings = null, bool commit = true)
    {
        txModeSettings ??= new TxModeSerializableSettings();

        var txSettings = GetTransactionSettings(txModeSettings);

        var tx = new Tx(new TransactionControl { BeginTx = txSettings, CommitTx = commit });
        return tx;
    }

    private static TransactionSettings GetTransactionSettings(ITxModeSettings txModeSettings)
    {
        var txSettings = txModeSettings switch
        {
            TxModeSerializableSettings => new TransactionSettings
            {
                SerializableReadWrite = new SerializableModeSettings()
            },
            TxModeOnlineSettings onlineModeSettings => new TransactionSettings
            {
                OnlineReadOnly = new OnlineModeSettings
                {
                    AllowInconsistentReads = onlineModeSettings.AllowInconsistentReads
                }
            },
            TxModeStaleSettings => new TransactionSettings
            {
                StaleReadOnly = new StaleModeSettings()
            },
            TxModeSnapshotSettings => new TransactionSettings
            {
                SnapshotReadOnly = new SnapshotModeSettings()
            },
            _ => throw new InvalidCastException(nameof(txModeSettings))
        };
        return txSettings;
    }

    public async Task<QueryResponseWithResult<T>> Query<T>(string queryString, Dictionary<string, YdbValue> parameters,
        Func<ExecuteQueryStream, Task<T>> func, ExecuteQuerySettings? executeQuerySettings = null)
    {
        var stream = QueryClient.ExecuteQuery(SessionId, queryString, this, parameters,
            executeQuerySettings);
        try
        {
            var response = await func(stream);
            return response is QueryClient.None
                ? new QueryResponseWithResult<T>(Status.Success)
                : new QueryResponseWithResult<T>(Status.Success, response);
        }
        catch (StatusUnsuccessfulException e)
        {
            return new QueryResponseWithResult<T>(e.Status);
        }
    }

    public async Task<QueryResponseWithResult<T>> Query<T>(string queryString, Func<ExecuteQueryStream, Task<T>> func,
        ExecuteQuerySettings? executeQuerySettings = null)
    {
        return await Query(queryString, new Dictionary<string, YdbValue>(), func, executeQuerySettings);
    }

    public async Task<QueryResponse> Query(string queryString, Dictionary<string, YdbValue> parameters,
        Func<ExecuteQueryStream, Task> func, ExecuteQuerySettings? executeQuerySettings = null)
    {
        var response = await Query<QueryClient.None>(
            queryString,
            parameters,
            async session =>
            {
                await func(session);
                return QueryClient.None.Instance;
            },
            executeQuerySettings);
        return response;
    }

    public async Task<QueryResponse> Query(string queryString, Func<ExecuteQueryStream, Task> func,
        ExecuteQuerySettings? executeQuerySettings = null)
    {
        return await Query(queryString, new Dictionary<string, YdbValue>(), func, executeQuerySettings);
    }

    internal QueryClient QueryClient { private get; set; }
    internal string SessionId { private get; set; }
}