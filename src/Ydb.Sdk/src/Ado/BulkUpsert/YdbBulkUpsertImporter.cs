using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.BulkUpsert;

public sealed class YdbBulkUpsertImporter<T> : IAsyncDisposable
{
    private readonly YdbConnection _connection;
    private readonly string _tablePath;
    private readonly IReadOnlyList<string> _columns;
    private readonly IReadOnlyList<Func<T, YdbValue>> _selectors;
    private readonly int _maxRowsInBatch;
    private readonly List<T> _buffer = new();
    private bool _isCompleted;

    public YdbBulkUpsertImporter(
        YdbConnection connection,
        string tablePath,
        IReadOnlyList<string> columns,
        IReadOnlyList<Func<T, YdbValue>> selectors,
        int maxRowsInBatch = 1000)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _tablePath = tablePath ?? throw new ArgumentNullException(nameof(tablePath));
        _columns = columns ?? throw new ArgumentNullException(nameof(columns));
        _selectors = selectors ?? throw new ArgumentNullException(nameof(selectors));
        _maxRowsInBatch = maxRowsInBatch;
    }

    public async Task WriteRowAsync(T row, CancellationToken cancellationToken = default)
    {
        if (_isCompleted)
            throw new InvalidOperationException("BulkUpsertImporter уже завершён.");

        _buffer.Add(row);

        if (_buffer.Count >= _maxRowsInBatch)
        {
            await FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        if (_buffer.Count == 0)
            return;

        await _connection.BulkUpsertWithRetry(
            _tablePath,
            _buffer,
            _columns,
            _selectors,
            cancellationToken
        ).ConfigureAwait(false);

        _buffer.Clear();
    }

    public async Task CompleteAsync(CancellationToken cancellationToken = default)
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