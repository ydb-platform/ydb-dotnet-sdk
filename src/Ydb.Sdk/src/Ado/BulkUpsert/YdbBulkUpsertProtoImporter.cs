using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.BulkUpsert;

public sealed class YdbBulkUpsertProtoImporter : IAsyncDisposable
{
    private readonly YdbConnection _connection;
    private readonly string _tablePath;
    private readonly List<string> _columns;
    private readonly List<Type> _types;
    private readonly int _maxBytes;

    private readonly List<Ydb.Value> _rows = new();
    private bool _disposed;

    public YdbBulkUpsertProtoImporter(
        YdbConnection connection,
        string tablePath,
        IReadOnlyList<string> columns,
        IReadOnlyList<Type> types,
        int maxBytes = 1024 * 1024)
    {
        _connection = connection;
        _tablePath = tablePath;
        _columns = columns is List<string> colList ? colList : new List<string>(columns);
        _types = types is List<Type> typList ? typList : new List<Type>(types);
        _maxBytes = maxBytes;
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

    public async ValueTask AddRowsAsync(IEnumerable<YdbValue[]> rows, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        foreach (var values in rows)
        {
            if (values.Length != _columns.Count)
                throw new ArgumentException("Values count must match columns count", nameof(values));

            var dict = _columns.Zip(values, (name, value) => new KeyValuePair<string, YdbValue>(name, value))
                .ToDictionary(x => x.Key, x => x.Value);

            var structValue = YdbValue.MakeStruct(dict).GetProto().Value;
            _rows.Add(structValue);

            var totalSize = _rows.Sum(r => r.CalculateSize());

            if (totalSize >= _maxBytes)
                await FlushAsync(cancellationToken);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        await FlushAsync();
        _disposed = true;
    }

    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        if (_rows.Count == 0)
            return;

        await _connection.BulkUpsertProtoAsync(
            _tablePath,
            GetStructType(),
            new List<Ydb.Value>(_rows),
            cancellationToken);

        _rows.Clear();
    }

    private Type GetStructType()
    {
        var structType = new Type { StructType = new StructType() };
        for (var i = 0; i < _columns.Count; i++)
        {
            structType.StructType.Members.Add(new StructMember
            {
                Name = _columns[i],
                Type = _types[i]
            });
        }

        return structType;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(YdbBulkUpsertProtoImporter));
    }

    public IReadOnlyList<Ydb.Value> GetBufferedRows() => _rows;
}
