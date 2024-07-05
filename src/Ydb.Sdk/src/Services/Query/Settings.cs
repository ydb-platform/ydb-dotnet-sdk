using Ydb.Query;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query;

public enum Syntax
{
    Unspecified = 0,

    /// <summary>
    /// YQL
    /// </summary>
    YqlV1 = 1,

    /// <summary>
    /// PostgresSQL
    /// </summary>
    Pg = 2
}

public class ExecuteQuerySettings : GrpcRequestSettings
{
    internal new static readonly ExecuteQuerySettings DefaultInstance = new();

    public Syntax Syntax { get; set; } = Syntax.YqlV1;
    public bool ConcurrentResultSets { get; set; }
}

public enum TxMode
{
    NoTx,

    SerializableRw,
    SnapshotRo,
    StaleRo,

    OnlineRo,
    OnlineInconsistentRo
}

internal static class TxModeExtensions
{
    private static readonly TransactionSettings SerializableRw = new()
        { SerializableReadWrite = new SerializableModeSettings() };

    private static readonly TransactionSettings SnapshotRo = new()
        { SnapshotReadOnly = new SnapshotModeSettings() };

    private static readonly TransactionSettings StaleRo = new()
        { StaleReadOnly = new StaleModeSettings() };

    private static readonly TransactionSettings OnlineRo = new()
        { OnlineReadOnly = new OnlineModeSettings { AllowInconsistentReads = false } };

    private static readonly TransactionSettings OnlineInconsistentRo = new()
        { OnlineReadOnly = new OnlineModeSettings { AllowInconsistentReads = true } };

    internal static TransactionSettings? TransactionSettings(this TxMode mode)
    {
        return mode switch
        {
            TxMode.SerializableRw => SerializableRw,
            TxMode.SnapshotRo => SnapshotRo,
            TxMode.StaleRo => StaleRo,
            TxMode.OnlineRo => OnlineRo,
            TxMode.OnlineInconsistentRo => OnlineInconsistentRo,
            _ => null
        };
    }

    internal static TransactionControl? TransactionControl(this TxMode mode, bool commit = true)
    {
        return mode switch
        {
            TxMode.NoTx => null,
            _ => new TransactionControl { BeginTx = mode.TransactionSettings(), CommitTx = commit }
        };
    }
}

public class ExecuteQueryPart
{
    public Value.ResultSet? ResultSet { get; }
    public string? TxId { get; }

    public long ResultSetIndex { get; }

    internal ExecuteQueryPart(ExecuteQueryResponsePart part)
    {
        ResultSet = part.ResultSet?.FromProto();
        TxId = part.TxMeta.Id;
        ResultSetIndex = part.ResultSetIndex;
    }
}

public sealed class ExecuteQueryStream : IAsyncEnumerator<ExecuteQueryPart>, IAsyncEnumerable<ExecuteQueryPart>
{
    private readonly IAsyncEnumerator<ExecuteQueryResponsePart> _stream;
    private readonly Action<string> _onTxId;

    internal ExecuteQueryStream(IAsyncEnumerator<ExecuteQueryResponsePart> stream, Action<string>? onTx = null)
    {
        _stream = stream;
        _onTxId = onTx ?? (_ => { });
    }

    public ValueTask DisposeAsync()
    {
        return _stream.DisposeAsync();
    }

    public async ValueTask<bool> MoveNextAsync()
    {
        var isNext = await _stream.MoveNextAsync();

        if (!isNext)
        {
            return isNext;
        }

        Status.FromProto(_stream.Current.Status, _stream.Current.Issues).EnsureSuccess();

        _onTxId.Invoke(_stream.Current.TxMeta.Id);

        return isNext;
    }

    public ExecuteQueryPart Current => new(_stream.Current);

    public IAsyncEnumerator<ExecuteQueryPart> GetAsyncEnumerator(CancellationToken cancellationToken = new())
    {
        return this;
    }
}
