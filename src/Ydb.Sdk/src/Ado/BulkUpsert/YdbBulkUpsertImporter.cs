using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.BulkUpsert;

public sealed class BulkUpsertImporter : IBulkUpsertImporter
{
    private readonly YdbConnection _connection;
    private readonly string _tablePath;
    private readonly List<string> _columns;
    private readonly List<Type> _types;
    private readonly int _maxBytes;
    private readonly List<Ydb.Value> _rows = new();
    private bool _disposed;

    public BulkUpsertImporter(
        YdbConnection connection,
        string tablePath,
        IReadOnlyList<string> columns,
        IReadOnlyList<Type> types,
        int maxBytes = 1024 * 1024)
    {
        _connection = connection;
        _tablePath = tablePath;
        _columns = columns.ToList();
        _types = types.ToList();
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

    public async ValueTask AddRowAsync(params object?[] values)
    {
        ThrowIfDisposed();
        if (values.Length != _columns.Count)
            throw new ArgumentException("Values count must match columns count", nameof(values));

        var ydbValues = new YdbValue[values.Length];
        for (int i = 0; i < values.Length; i++)
        {
            ydbValues[i] = YdbValueFromObject(values[i], _types[i]);
        }
        await AddRowAsync(ydbValues);
    }

    public async ValueTask AddRowsAsync(IEnumerable<YdbValue[]> rows, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        foreach (var values in rows)
            await AddRowAsync(values);
    }

    public async ValueTask AddRowsAsync(IEnumerable<object?[]> rows, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        foreach (var values in rows)
            await AddRowAsync(values);
    }

    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (_rows.Count == 0) return;

        await _connection.BulkUpsertProtoAsync(_tablePath, GetStructType(), _rows.ToList(), cancellationToken);
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

    private static YdbValue YdbValueFromObject(object? value, Type columnType)
    {
        switch (value)
        {
            case YdbValue ydbValue:
                return ydbValue;
            default:
                switch (columnType.TypeId)
                {
                    case Type.Types.PrimitiveTypeId.Int32:
                        return YdbValue.MakeInt32(Convert.ToInt32(value));
                    case Type.Types.PrimitiveTypeId.Utf8:
                        return YdbValue.MakeUtf8(value?.ToString()!);
                    default:
                        throw new NotSupportedException($"Type '{columnType.TypeId}' not supported in YdbValueFromObject");
                }
        }
    }
}