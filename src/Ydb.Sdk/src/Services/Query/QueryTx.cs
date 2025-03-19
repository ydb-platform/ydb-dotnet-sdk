using System.Collections.Immutable;
using Ydb.Query;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query;

public class QueryTx
{
    private readonly Session _session;
    private readonly TxMode _txMode;

    private string? TxId { get; set; }
    private bool Commited { get; set; }

    private TransactionControl? TxControl(bool commit)
    {
        Commited = commit | Commited;

        return TxId == null
            ? _txMode.TransactionControl(commit: commit)
            : new TransactionControl { TxId = TxId, CommitTx = commit };
    }

    internal QueryTx(Session session, TxMode txMode)
    {
        _session = session;
        _txMode = txMode;
    }

    public async ValueTask<ExecuteQueryStream> Stream(string query, Dictionary<string, YdbValue>? parameters = null,
        bool commit = false, ExecuteQuerySettings? settings = null) =>
        new(
            await _session.ExecuteQuery(query, parameters, settings, TxControl(commit)), txId => TxId = txId
        );

    public async Task<IReadOnlyList<Value.ResultSet.Row>> ReadAllRows(string query,
        Dictionary<string, YdbValue>? parameters = null, bool commit = false, ExecuteQuerySettings? settings = null)
    {
        await using var stream = await Stream(query, parameters, commit, settings);
        List<Value.ResultSet.Row> rows = new();

        await foreach (var part in stream)
        {
            if (part.ResultSetIndex > 0)
            {
                break;
            }

            rows.AddRange(part.ResultSet?.Rows ?? ImmutableList<Value.ResultSet.Row>.Empty);
        }

        return rows.AsReadOnly();
    }

    public async Task<Value.ResultSet.Row?> ReadRow(string query, Dictionary<string, YdbValue>? parameters = null,
        bool commit = false, ExecuteQuerySettings? settings = null)
    {
        var result = await ReadAllRows(query, parameters, commit, settings);

        return result is { Count: > 0 } ? result[0] : null;
    }

    public async Task Exec(string query, Dictionary<string, YdbValue>? parameters = null,
        ExecuteQuerySettings? settings = null, bool commit = false)
    {
        await using var stream = await Stream(query, parameters, commit, settings);
        await stream.MoveNextAsync();
    }

    public async Task Rollback()
    {
        if (TxId == null)
        {
            return;
        }

        Commited = true;

        var status = await _session.RollbackTransaction(TxId!);
        status.EnsureSuccess();
    }

    internal async Task Commit()
    {
        if (!Commited)
        {
            await _session.CommitTransaction(TxId!);
        }
    }
}
