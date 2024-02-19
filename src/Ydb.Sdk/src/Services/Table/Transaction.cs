using Microsoft.Extensions.Logging;
using Ydb.Table;

namespace Ydb.Sdk.Services.Table;

public class Transaction
{
    internal Transaction(string txId)
    {
        TxId = txId;
    }

    public string TxId { get; }

    internal int? TxNum { get; private set; }
    private static int _txCounter;

    internal static Transaction? FromProto(TransactionMeta proto, ILogger? logger = null)
    {
        if (proto.Id.Length == 0)
        {
            return null;
        }

        var tx = new Transaction(
            txId: proto.Id);
        if (!string.IsNullOrEmpty(proto.Id))
        {
            tx.TxNum = IncTxCounter();
            logger.LogTrace($"Received tx #{tx.TxNum}");
        }

        return tx;
    }
    
    private static int IncTxCounter()
    {
        return Interlocked.Increment(ref _txCounter);
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

    private readonly int? _txNum;

    private TxControl(TransactionControl proto, int? txNum = null)
    {
        _proto = proto;
        _txNum = txNum;
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
        }, tx.TxNum);
    }

    public TxControl Commit()
    {
        _proto.CommitTx = true;
        return this;
    }

    internal TransactionControl ToProto(ILogger? logger = null)
    {
        if (_txNum != null)
        {
            logger.LogTrace($"Using tx #{_txNum}");
        }

        return _proto.Clone();
    }
}
