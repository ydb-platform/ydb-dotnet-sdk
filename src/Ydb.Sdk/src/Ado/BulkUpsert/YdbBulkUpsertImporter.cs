using Ydb.Sdk.Services.Table;

namespace Ydb.Sdk.Ado.BulkUpsert;

public sealed class YdbBulkUpsertImporter<T> : IAsyncDisposable
{
    private readonly YdbConnection _connection;
    private readonly string _tablePath;
    private readonly IReadOnlyList<string> _columns;
    private readonly Func<T, object?>[] _selectors;
    private readonly List<T> _buffer = new();
    private bool _isCompleted;
    private readonly int _maxRowsInBatch;

    public YdbBulkUpsertImporter(
        YdbConnection connection,
        string tablePath,
        IReadOnlyList<string> columns,
        int maxRowsInBatch = 1000)
    {
        _connection = connection;
        _tablePath = tablePath;
        _columns = columns;
        _maxRowsInBatch = maxRowsInBatch;

        var props = columns.Select(name =>
            typeof(T).GetProperty(name) ??
            throw new ArgumentException($"Type {typeof(T).Name} does not have a property '{name}'")
        ).ToArray();
        _selectors = props.Select(p => (Func<T, object?>)(x => p.GetValue(x))).ToArray();
    }

    public async Task WriteRowAsync(T row, CancellationToken cancellationToken = default)
    {
        if (_isCompleted)
            throw new InvalidOperationException("BulkUpsertImporter уже завершён.");

        _buffer.Add(row);

        if (_buffer.Count >= _maxRowsInBatch)
            await FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        if (_buffer.Count == 0)
            return;

        await _connection.BulkUpsertWithRetry(
            _tablePath,
            _buffer,
            _columns,
            cancellationToken
        ).ConfigureAwait(false);

        _buffer.Clear();
    }

    private async Task CompleteAsync(CancellationToken cancellationToken = default)
    {
        if (_isCompleted) return;
        await FlushAsync(cancellationToken).ConfigureAwait(false);
        _isCompleted = true;
    }

    public async ValueTask DisposeAsync()
    {
        if (!_isCompleted)
            await CompleteAsync();
    }
}