using Ydb.Sdk.Value;
using Ydb.Sdk.Ado.Internal;
using Ydb.Table;
using Ydb.Table.V1;

namespace Ydb.Sdk.Ado.BulkUpsert;

public sealed class BulkUpsertImporter : IBulkUpsertImporter
{
    private readonly YdbConnection _connection;
    private readonly string _tablePath;
    private readonly IReadOnlyList<string> _columns;
    private readonly int _maxBytes;
    private readonly List<YdbValue> _rows = new();
    private readonly CancellationToken _cancellationToken;
    private bool _disposed;

    public BulkUpsertImporter(
        YdbConnection connection,
        string tableName,
        IReadOnlyList<string> columns,
        CancellationToken cancellationToken = default,
        int maxBytes = 64 * 1024 * 1024)
    {
        _connection = connection;
        _tablePath = tableName;
        _columns = columns;
        _maxBytes = maxBytes;
        _cancellationToken = cancellationToken;
    }

    public async ValueTask AddRowsAsync(IEnumerable<object?[]> rows)
    {
        ThrowIfDisposed();
        foreach (var values in rows)
        {
            if (values.Length != _columns.Count)
                throw new ArgumentException("Values count must match columns count", nameof(values));

            var ydbValues = values.Select(v =>
                v is YdbValue yv ? yv :
                v is YdbParameter param ? param.YdbValue :
                throw new ArgumentException("All values must be either YdbValue or YdbParameter")).ToArray();

            var dict = _columns.Zip(ydbValues, (name, value) => new KeyValuePair<string, YdbValue>(name, value))
                .ToDictionary(x => x.Key, x => x.Value);

            var structRow = YdbValue.MakeStruct(dict);
            _rows.Add(structRow);

            var totalSize = _rows.Sum(r => r.GetProto().Value.CalculateSize());
            if (totalSize >= _maxBytes)
                await FlushAsync(_cancellationToken);
        }
    }

    public async ValueTask AddRowsAsync(IEnumerable<YdbValue[]> rows)
    {
        ThrowIfDisposed();
        foreach (var values in rows)
        {
            if (values.Length != _columns.Count)
                throw new ArgumentException("Values count must match columns count", nameof(values));

            var dict = _columns.Zip(values, (name, value) => new KeyValuePair<string, YdbValue>(name, value))
                .ToDictionary(x => x.Key, x => x.Value);

            var structRow = YdbValue.MakeStruct(dict);
            _rows.Add(structRow);

            var totalSize = _rows.Sum(r => r.GetProto().Value.CalculateSize());
            if (totalSize >= _maxBytes)
                await FlushAsync(_cancellationToken);
        }
    }

    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (_rows.Count == 0) return;

        var structType = _rows[0].GetProto().Type;

        var listValue = new Ydb.Value();
        foreach (var row in _rows)
            listValue.Items.Add(row.GetProto().Value);

        var typedValue = new TypedValue
        {
            Type  = new Type { ListType = new ListType { Item = structType } },
            Value = listValue
        };

        var req = new BulkUpsertRequest { Table = _tablePath, Rows = typedValue };

        var resp = await _connection.Session.Driver.UnaryCall(
            TableService.BulkUpsertMethod,
            req,
            new GrpcRequestSettings { CancellationToken = cancellationToken }
        ).ConfigureAwait(false);

        var operation = resp.Operation;
        if (operation.Status.IsNotSuccess())
            throw YdbException.FromServer(operation.Status, operation.Issues);

        _rows.Clear();
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        await FlushAsync();
        _disposed = true;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(BulkUpsertImporter));
    }
}
