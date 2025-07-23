using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ydb.Sdk.Client;
using Ydb.Sdk.Services.Table;

namespace Ydb.Sdk.Ado.BulkUpsert;

public sealed class YdbBulkUpsertImporter<T> : IAsyncDisposable
{
    private readonly TableClient _tableClient;
    private readonly string _tablePath;
    private readonly BulkUpsertOptions _options;
    private readonly RetrySettings? _retrySettings;
    private readonly int _maxRowsInBatch;
    private readonly List<T> _buffer = new();
    private bool _isCompleted;

    public YdbBulkUpsertImporter(
        TableClient tableClient,
        string tablePath,
        BulkUpsertOptions? options = null,
        RetrySettings? retrySettings = null,
        int maxRowsInBatch = 1000)
    {
        _tableClient = tableClient ?? throw new ArgumentNullException(nameof(tableClient));
        _tablePath = tablePath ?? throw new ArgumentNullException(nameof(tablePath));
        _options = options ?? new BulkUpsertOptions();
        _retrySettings = retrySettings;
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

        await _tableClient.BulkUpsertWithRetry(
            _tablePath,
            _buffer,
            _retrySettings
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