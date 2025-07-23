using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ydb.Sdk.Ado.BulkUpsert;

namespace Ydb.Sdk.Ado
{
    public sealed class YdbBulkUpsertImporter<T> : IAsyncDisposable
    {
        private readonly YdbConnection _connection;
        private readonly string _tablePath;
        private readonly BulkUpsertOptions _options;
        private readonly List<T> _buffer = new();
        private readonly int _maxBatchSizeBytes;
        private int _bufferSizeBytes;
        private bool _isCompleted;

        public YdbBulkUpsertImporter(
            YdbConnection connection,
            string tablePath,
            BulkUpsertOptions? options = null,
            int maxBatchSizeBytes = 64 * 1024 * 1024)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _tablePath = tablePath ?? throw new ArgumentNullException(nameof(tablePath));
            _options = options ?? new BulkUpsertOptions();
            _maxBatchSizeBytes = maxBatchSizeBytes;
        }

        /// <summary>
        /// Записывает одну строку. При достижении лимита буфера — автоматически отправляет батч.
        /// </summary>
        public async Task WriteRowAsync(T row, CancellationToken cancellationToken = default)
        {
            if (_isCompleted)
                throw new InvalidOperationException("BulkUpsertImporter уже завершён.");

            _buffer.Add(row);

            // Быстро оцениваем размер через сериализацию одного row в TypedValue
            var rowProto = TypedValueFactory.FromObjects(new[] { row });
            var rowSize = rowProto.CalculateSize();

            _bufferSizeBytes += rowSize;

            if (_bufferSizeBytes >= _maxBatchSizeBytes)
            {
                await FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Отправляет накопленные строки в YDB.
        /// </summary>
        public async Task FlushAsync(CancellationToken cancellationToken = default)
        {
            if (_buffer.Count == 0)
                return;

            await _connection.BulkUpsertInternalAsync(_tablePath, _buffer, _options, cancellationToken).ConfigureAwait(false);

            _buffer.Clear();
            _bufferSizeBytes = 0;
        }

        /// <summary>
        /// Завершает импорт: отправляет оставшиеся строки.
        /// </summary>
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
}