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
    public Syntax Syntax { get; set; } = Syntax.YqlV1;
    public bool ConcurrentResultSets { get; set; }
}

public class ExecuteQueryPart
{
    public Value.ResultSet? ResultSet { get; }
    public long ResultSetIndex { get; }

    internal ExecuteQueryPart(ExecuteQueryResponsePart part)
    {
        ResultSet = part.ResultSet?.FromProto();
        ResultSetIndex = part.ResultSetIndex;
    }
}

public sealed class ExecuteQueryStream : IAsyncEnumerator<ExecuteQueryPart>, IAsyncEnumerable<ExecuteQueryPart>
{
    private readonly IServerStream<ExecuteQueryResponsePart> _stream;
    private readonly Action<string?> _onTxId;

    internal ExecuteQueryStream(IServerStream<ExecuteQueryResponsePart> stream, Action<string?>? onTx = null)
    {
        _stream = stream;
        _onTxId = onTx ?? (_ => { });
    }

    public ValueTask DisposeAsync()
    {
        _stream.Dispose();

        return default;
    }

    public async ValueTask<bool> MoveNextAsync()
    {
        var isNext = await _stream.MoveNextAsync();

        if (!isNext)
        {
            return isNext;
        }

        Status.FromProto(_stream.Current.Status, _stream.Current.Issues).EnsureSuccess();

        _onTxId.Invoke(_stream.Current.TxMeta?.Id);

        return isNext;
    }

    public ExecuteQueryPart Current => new(_stream.Current);

    public IAsyncEnumerator<ExecuteQueryPart> GetAsyncEnumerator(CancellationToken cancellationToken = new()) => this;
}
