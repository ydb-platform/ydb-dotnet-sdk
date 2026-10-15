using System.Data;
using System.Diagnostics.Metrics;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.RetryPolicy;

namespace Rmw;

public sealed class AdoRmwStore : IRmwStore, IDisposable
{
    private readonly YdbDataSource _client;
    private readonly int _writeTimeout;
    private readonly int _readTimeout;
    private readonly AsyncLocal<bool> _writing = new();
    private readonly MeterListener _retryListener = new();

    public AdoRmwStore(YdbDataSource client, int writeTimeout, int readTimeout,
        Action<int>? recordAttempts = null)
    {
        _client = client;
        _writeTimeout = writeTimeout;
        _readTimeout = readTimeout;
        _retryListener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "Ydb.Sdk" && instrument.Name == "ydb.client.retry.attempts")
                listener.EnableMeasurementEvents(instrument);
        };
        _retryListener.SetMeasurementEventCallback<int>((_, attempts, _, _) =>
        {
            // The SDK reports after its outer retry loop, including acquisition and commit attempts.
            if (_writing.Value) recordAttempts?.Invoke(attempts);
        });
        _retryListener.Start();
    }

    public void Dispose() => _retryListener.Dispose();

    // These names belong to this instance's entire create/run/check/drop lifecycle.
    public string Namespace { get; } = $"slo_rmw_{Guid.NewGuid():N}";
    private string Ledger => $"{Namespace}_ledger";
    private string Counters => $"{Namespace}_counters";
    private bool _ledgerCreated;
    private bool _countersCreated;

    public async Task Create(int counters)
    {
        await _client.ExecuteAsync(async connection =>
        {
            await using var command = new YdbCommand($"""
                                                      CREATE TABLE `{Ledger}` (
                                                          OpId Text NOT NULL, CounterId Int32 NOT NULL, Delta Int64 NOT NULL,
                                                          PRIMARY KEY (OpId)
                                                      )
                                                      """, connection);
            command.CommandTimeout = _writeTimeout;
            await command.ExecuteNonQueryAsync();
            _ledgerCreated = true;
        });
        await _client.ExecuteAsync(async connection =>
        {
            await using var command = new YdbCommand($"""
                                                      CREATE TABLE `{Counters}` (
                                                          CounterId Int32 NOT NULL, Value Int64 NOT NULL, Version Int64 NOT NULL,
                                                          PRIMARY KEY (CounterId)
                                                      )
                                                      """, connection);
            command.CommandTimeout = _writeTimeout;
            await command.ExecuteNonQueryAsync();
            _countersCreated = true;
        });
        await _client.ExecuteAsync(async connection =>
        {
            for (var id = 0; id < counters; id++)
            {
                await using var command = new YdbCommand($"""
                                                          UPSERT INTO `{Counters}` (CounterId, Value, Version) VALUES (@id, 0l, 0l)
                                                          """, connection);
                command.CommandTimeout = _writeTimeout;
                command.Parameters.Add(new YdbParameter("id", DbType.Int32, id));
                await command.ExecuteNonQueryAsync();
            }
        });
    }

    public async Task ExecuteTransaction(Func<IRmwTransaction, Task> callback)
    {
        _writing.Value = true;
        try
        {
            await _client.ExecuteInTransactionAsync(
                connection => callback(new Transaction(connection, Ledger, Counters, _writeTimeout)),
                YdbRetryPolicy.IdempotenceDefault);
        }
        finally
        {
            _writing.Value = false;
        }
    }

    public Task<RmwSnapshot> ReadSnapshot() => _client.ExecuteInTransactionAsync(async connection =>
    {
        var operations = new List<RmwOperation>();
        var counters = new List<RmwCounter>();
        await using var command = new YdbCommand($"""
                                                  SELECT OpId, CounterId, Delta FROM `{Ledger}`;
                                                  SELECT CounterId, Value, Version FROM `{Counters}`;
                                                  """, connection);
        command.CommandTimeout = _readTimeout;
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            operations.Add(new RmwOperation(reader.GetString(0), reader.GetInt32(1), reader.GetInt64(2)));
        if (!await reader.NextResultAsync())
            throw new InvalidOperationException("RMW snapshot has no counters result");
        while (await reader.ReadAsync())
            counters.Add(new RmwCounter(reader.GetInt32(0), reader.GetInt64(1), reader.GetInt64(2)));
        return new RmwSnapshot(operations, counters);
    }, YdbRetryPolicy.IdempotenceDefault);

    public async Task Drop()
    {
        try
        {
            if (_ledgerCreated) await _client.DropTable(Ledger);
        }
        finally
        {
            if (_countersCreated) await _client.DropTable(Counters);
        }
    }

    private sealed class Transaction(YdbConnection connection, string ledger, string counters, int timeout)
        : IRmwTransaction
    {
        public async Task<RmwOperation?> ReadOperation(string id)
        {
            await using var command = new YdbCommand(
                $"SELECT OpId, CounterId, Delta FROM `{ledger}` WHERE OpId = @id", connection);
            command.CommandTimeout = timeout;
            command.Parameters.Add(new YdbParameter("id", DbType.String, id));
            await using var reader = await command.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return null;
            var result = new RmwOperation(reader.GetString(0), reader.GetInt32(1), reader.GetInt64(2));
            if (await reader.ReadAsync()) throw new InvalidOperationException("RMW duplicate operation");
            return result;
        }

        public async Task<RmwCounter> ReadCounter(int id)
        {
            await using var command = new YdbCommand(
                $"SELECT CounterId, Value, Version FROM `{counters}` WHERE CounterId = @id", connection);
            command.CommandTimeout = timeout;
            command.Parameters.Add(new YdbParameter("id", DbType.Int32, id));
            await using var reader = await command.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) throw new InvalidOperationException($"RMW missing counter: {id}");
            var result = new RmwCounter(reader.GetInt32(0), reader.GetInt64(1), reader.GetInt64(2));
            if (await reader.ReadAsync()) throw new InvalidOperationException("RMW duplicate counter");
            return result;
        }

        public async Task Write(RmwOperation operation, RmwCounter counter)
        {
            await using var command = new YdbCommand($"""
                                                      UPSERT INTO `{counters}` (CounterId, Value, Version) VALUES (@counter, @value, @version);
                                                      INSERT INTO `{ledger}` (OpId, CounterId, Delta) VALUES (@id, @counter, @delta);
                                                      """, connection);
            command.CommandTimeout = timeout;
            command.Parameters.Add(new YdbParameter("id", DbType.String, operation.Id));
            command.Parameters.Add(new YdbParameter("counter", DbType.Int32, operation.Counter));
            command.Parameters.Add(new YdbParameter("delta", DbType.Int64, operation.Delta));
            command.Parameters.Add(new YdbParameter("value", DbType.Int64, counter.Value));
            command.Parameters.Add(new YdbParameter("version", DbType.Int64, counter.Version));
            await command.ExecuteNonQueryAsync();
        }
    }
}