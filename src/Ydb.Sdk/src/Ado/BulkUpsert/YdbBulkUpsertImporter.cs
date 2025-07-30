using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Value;
using Ydb.Table;
using Ydb.Table.V1;

namespace Ydb.Sdk.Ado.BulkUpsert;

public sealed class BulkUpsertImporter : IBulkUpsertImporter
{
    private readonly YdbConnection _connection;
    private readonly string _tablePath;
    private readonly IReadOnlyList<string> _columns;
    private readonly IReadOnlyList<Type> _types;
    private readonly int _maxBytes;
    private readonly List<Ydb.Value> _rows = new();
    private bool _disposed;

    public BulkUpsertImporter(
        YdbConnection connection,
        string tablePath,
        IReadOnlyList<string> columns,
        IReadOnlyList<Type> types,
        int maxBytes = 64 * 1024 * 1024
    )
    {
        _connection = connection;
        _tablePath = tablePath;
        _columns = columns;
        _types = types;
        _maxBytes = maxBytes;
    }

    public async ValueTask AddRowAsync(params object?[] values)
    {
        ThrowIfDisposed();
        if (values.Length != _columns.Count)
            throw new ArgumentException("Values count must match columns count", nameof(values));

        var ydbValues = values.Select(v =>
            v is YdbValue yv ? yv :
            v is YdbParameter param ? param.YdbValue :
            throw new ArgumentException("All values must be either YdbValue or YdbParameter")).ToArray();

        await AddRowAsync(ydbValues);
    }

    public async ValueTask AddRowAsync(params YdbValue[] values)
    {
        ThrowIfDisposed();
        if (values.Length != _columns.Count)
            throw new ArgumentException("Values count must match columns count", nameof(values));

        var dict = _columns.Zip(values, (name, value) => new KeyValuePair<string, YdbValue>(name, value))
            .ToDictionary(x => x.Key, x => x.Value);

        var structValue = YdbValue.MakeStruct(dict).GetProto().Value;
        _rows.Add(structValue);

        var totalSize = _rows.Sum(r => r.CalculateSize());
        if (totalSize >= _maxBytes)
            await FlushAsync();
    }

    public async ValueTask AddRowsAsync(IEnumerable<object?[]> rows, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        foreach (var values in rows)
            await AddRowAsync(values);
    }

    public async ValueTask AddRowsAsync(IEnumerable<YdbValue[]> rows, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        foreach (var values in rows)
            await AddRowAsync(values);
    }

    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (_rows.Count == 0) return;

        var listValue = new Ydb.Value();
        listValue.Items.AddRange(_rows);

        var typedValue = new TypedValue { Type = GetStructType(), Value = listValue };
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

    public IReadOnlyList<Ydb.Value> GetBufferedRows() => _rows;

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        await FlushAsync();
        _disposed = true;
    }

    private Type GetStructType()
    {
        var structType = new Type { StructType = new StructType() };
        for (var i = 0; i < _columns.Count; i++)
            structType.StructType.Members.Add(new StructMember { Name = _columns[i], Type = _types[i] });
        return structType;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(BulkUpsertImporter));
    }
}
