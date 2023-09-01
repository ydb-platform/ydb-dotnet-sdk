using Ydb.Table;

namespace Ydb.Sdk.Table;

public class Transaction
{
    internal Transaction(string txId)
    {
        TxId = txId;
    }

    public string TxId { get; }

    internal static Transaction? FromProto(TransactionMeta proto)
    {
        if (proto.Id.Length == 0)
        {
            return null;
        }

        return new Transaction(
            txId: proto.Id);
    }
}

public enum TransactionState
{
    Unknown = 0,
    Active = 1,
    Void = 2
}

public class TxControl
{
    private readonly TransactionControl _proto;

    private TxControl(TransactionControl proto)
    {
        _proto = proto;
    }

    public static TxControl BeginSerializableRW()
    {
        return new TxControl(new TransactionControl
        {
            BeginTx = new TransactionSettings
            {
                SerializableReadWrite = new SerializableModeSettings()
            }
        });
    }

    public static TxControl BeginOnlineRO(bool allowInconsistentReads = false)
    {
        return new TxControl(new TransactionControl
        {
            BeginTx = new TransactionSettings
            {
                OnlineReadOnly = new OnlineModeSettings
                {
                    AllowInconsistentReads = allowInconsistentReads
                }
            }
        });
        ;
    }

    public static TxControl BeginStaleRO()
    {
        return new TxControl(new TransactionControl
        {
            BeginTx = new TransactionSettings
            {
                StaleReadOnly = new StaleModeSettings()
            }
        });
    }

    public static TxControl Tx(Transaction tx)
    {
        return new TxControl(new TransactionControl
        {
            TxId = tx.TxId
        });
    }

    public TxControl Commit()
    {
        _proto.CommitTx = true;
        return this;
    }

    internal TransactionControl ToProto()
    {
        return _proto.Clone();
    }
}