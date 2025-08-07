using System.Data;
using System.Data.Common;
using System.Text;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Retry;

internal sealed class InMemoryDbCommand : DbCommand
{
    private readonly YdbCommand _inner;
    private DataTable? _buffer;
    private static int  MaxBufferedRows  { get; set; } = 100000;
    private static long MaxBufferedBytes { get; set; } = 64 * 1024 * 1024;

    public InMemoryDbCommand(YdbCommand inner) => _inner = inner;

    public override string CommandText
    {
        get => _inner.CommandText;
        set => _inner.CommandText = value;
    }

    public override int CommandTimeout
    {
        get => _inner.CommandTimeout;
        set => _inner.CommandTimeout = value;
    }

    public override CommandType CommandType
    {
        get => _inner.CommandType;
        set => _inner.CommandType = value;
    }

    public override UpdateRowSource UpdatedRowSource
    {
        get => _inner.UpdatedRowSource;
        set => _inner.UpdatedRowSource = value;
    }

    protected override DbConnection DbConnection
    {
        get => _inner.Connection;
        set => _inner.Connection = (YdbConnection)value;
    }

    protected override DbParameterCollection DbParameterCollection => _inner.Parameters;

    protected override DbTransaction? DbTransaction
    {
        get => _inner.Transaction;
        set => _inner.Transaction = (YdbTransaction?)value;
    }

    public override bool DesignTimeVisible
    {
        get => _inner.DesignTimeVisible;
        set => _inner.DesignTimeVisible = value;
    }

    protected override DbParameter CreateDbParameter() => _inner.CreateParameter();

    public override void Prepare() => _inner.Prepare();
    public override void Cancel()  => _inner.Cancel();

    public override int ExecuteNonQuery()         => throw new NotSupportedException();
    public override object ExecuteScalar()       => throw new NotSupportedException();
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
        CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        await EnsureBufferAsync(behavior, cancellationToken).ConfigureAwait(false);
        return _buffer!.CreateDataReader();
    }

    private async Task EnsureBufferAsync(CommandBehavior behavior, CancellationToken ct)
    {
        if (_buffer is not null) return;
        _buffer = new DataTable();
        await using var rdr = await _inner.InternalExecuteDbDataReaderAsync(behavior, ct)
            .ConfigureAwait(false);
        _buffer.Load(rdr, LoadOption.OverwriteChanges, null);

        var approx = EstimateSize(_buffer);
        if (_buffer.Rows.Count > MaxBufferedRows || approx > MaxBufferedBytes)
            throw new InvalidOperationException(
                $"The result set is too large to retry in-memory " +
                $"(rows={_buffer.Rows.Count}, bytes≈{approx:N0}). " +
                $"Either lower fetch size, increase " +
                $"`InMemoryDbCommand.MaxBufferedRows/MaxBufferedBytes`, " +
                $"or disable retries for this command.");
    }

    private static long EstimateSize(DataTable t)
    {
        long total = 0;
        foreach (DataRow row in t.Rows)
        foreach (var obj in row.ItemArray)
            switch (obj)
            {
                case byte[] ba:
                    total += ba.Length;
                    break;
                case string s:
                    total += Encoding.UTF8.GetByteCount(s);
                    break;
                case DBNull:
                    break;
                default:
                    total += 8;
                    break;
            }

        return total;
    }
}
