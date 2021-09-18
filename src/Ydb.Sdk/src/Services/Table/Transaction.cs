namespace Ydb.Sdk.Table
{
    public class Transaction
    {
        internal Transaction(string txId)
        {
            TxId = txId;
        }

        public string TxId { get; }

        internal static Transaction? FromProto(Ydb.Table.TransactionMeta proto)
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
        Void = 2,
    }

    public class TxControl
    {
        private Ydb.Table.TransactionControl _proto;

        private TxControl(Ydb.Table.TransactionControl proto)
        {
            _proto = proto;
        }

        public static TxControl BeginSerializableRW()
        {
            return new TxControl(new Ydb.Table.TransactionControl
            {
                BeginTx = new Ydb.Table.TransactionSettings
                {
                    SerializableReadWrite = new Ydb.Table.SerializableModeSettings()
                }
            });
        }

        public static TxControl BeginOnlineRO(bool allowInconsistentReads = false)
        {
            return new TxControl(new Ydb.Table.TransactionControl
            {
                BeginTx = new Ydb.Table.TransactionSettings
                {
                    OnlineReadOnly = new Ydb.Table.OnlineModeSettings
                    {
                        AllowInconsistentReads = allowInconsistentReads
                    }
                }
            }); ;
        }

        public static TxControl BeginStaleRO()
        {
            return new TxControl(new Ydb.Table.TransactionControl
            {
                BeginTx = new Ydb.Table.TransactionSettings
                {
                    StaleReadOnly = new Ydb.Table.StaleModeSettings()
                }
            });
        }

        public static TxControl Tx(Transaction tx)
        {
            return new TxControl(new Ydb.Table.TransactionControl
            {
                TxId = tx.TxId
            });
        }

        public TxControl Commit()
        {
            _proto.CommitTx = true;
            return this;
        }

        internal Ydb.Table.TransactionControl ToProto()
        {
            return _proto.Clone();
        }
    }
}
