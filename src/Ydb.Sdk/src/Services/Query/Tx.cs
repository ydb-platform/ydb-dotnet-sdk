using Ydb.Query;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query;

public class RollbackTxResponse
{
}

public interface ITxModeSettings
{
}

public class SerializableModeSettings : ITxModeSettings
{
}

public class OnlineModeSettings : ITxModeSettings
{
    public OnlineModeSettings(bool allowInconsistentReads = false)
    {
        AllowInconsistentReads = allowInconsistentReads;
    }

    public bool AllowInconsistentReads { get; }
}

public class StaleModeSettings : ITxModeSettings
{
}

public class SnapshotModeSettings : ITxModeSettings
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
    }

    private Tx(TransactionControl proto)
    {
        _proto = proto;
    }

    internal TransactionControl ToProto()
    {
        return _proto;
    }

    public static Tx Begin(ITxModeSettings? txModeSettings = null, bool commit = true)
    {
        txModeSettings ??= new SerializableModeSettings();

        var txSettings = GetTransactionSettings(txModeSettings);

        var tx = new Tx(new TransactionControl { BeginTx = txSettings, CommitTx = commit });
        return tx;
    }

    private static TransactionSettings GetTransactionSettings(ITxModeSettings txModeSettings)
    {
        var txSettings = txModeSettings switch
        {
            SerializableModeSettings => new TransactionSettings
            {
                SerializableReadWrite = new Ydb.Query.SerializableModeSettings()
            },
            OnlineModeSettings onlineModeSettings => new TransactionSettings
            {
                OnlineReadOnly = new Ydb.Query.OnlineModeSettings
                {
                    AllowInconsistentReads = onlineModeSettings.AllowInconsistentReads
                }
            },
            StaleModeSettings => new TransactionSettings
            {
                StaleReadOnly = new Ydb.Query.StaleModeSettings()
            },
            SnapshotModeSettings => new TransactionSettings
            {
                SnapshotReadOnly = new Ydb.Query.SnapshotModeSettings()
            },
            _ => throw new InvalidCastException(nameof(txModeSettings))
        };
        return txSettings;
    }

    public async Task<ExecuteQueryStream> Query(string query, Dictionary<string, YdbValue> parameters)
    {
        throw new NotImplementedException();
    }
}