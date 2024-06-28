using System.Data;
using System.Data.Common;
using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Value;
using static System.Data.IsolationLevel;
using Session = Ydb.Sdk.Services.Query.Pool.Session;

namespace Ydb.Sdk.Ado;

public sealed class YdbConnection : DbConnection
{
    private YdbConnectionStringBuilder ConnectionStringBuilder { get; }

    internal Session Session { get; }

    internal YdbConnection(Session session, YdbConnectionStringBuilder connectionStringBuilder)
    {
        Session = session;
        ConnectionStringBuilder = connectionStringBuilder;
    }

    protected override YdbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        if (isolationLevel is not (Serializable or Unspecified))
        {
            throw new ArgumentException("Unsupported isolationLevel: " + isolationLevel);
        }

        return BeginTransaction();
    }

    public YdbTransaction BeginTransaction(TxMode txMode = TxMode.SerializableRw)
    {
        EnsureConnectionOpen();

        return new YdbTransaction(this, txMode);
    }

    public override void ChangeDatabase(string databaseName)
    {
    }

    public override void Close()
    {
        CloseAsync().GetAwaiter().GetResult();
    }

    public override void Open()
    {
    }

    public override async Task CloseAsync()
    {
        EnsureConnectionOpen();

        try
        {
            if (CurrentReader != null)
            {
                await CurrentReader.CloseAsync();

                CurrentReader = null;
                ConnectionState = ConnectionState.Closed;
            }
        }
        finally
        {
            Session.Release();
        }
    }

    public override string ConnectionString
    {
        get => ConnectionStringBuilder.ConnectionString;
#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
        set
#pragma warning restore CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
        {
            throw new NotImplementedException("Use YdbDataSource for creating connection");
        }
    }

    public override string Database => ConnectionStringBuilder.Database;

    public override ConnectionState State => ConnectionState;

    private ConnectionState ConnectionState { get; set; } = ConnectionState.Open;
    internal YdbDataReader? CurrentReader { get; set; }

    public override string DataSource => string.Empty; // TODO
    public override string ServerVersion => string.Empty; // TODO

    protected override YdbCommand CreateDbCommand()
    {
        EnsureConnectionOpen();

        return new YdbCommand(this);
    }

    internal async IAsyncEnumerator<(long, ResultSet?)> ExecuteQuery(
        string query,
        Dictionary<string, YdbValue> parameters,
        ExecuteQuerySettings executeQuerySettings,
        YdbTransaction? ydbTransaction = null)
    {
        EnsureConnectionOpen();

        if (CurrentReader != null)
        {
            throw new InvalidOperationException(
                "There is already an open YdbDataReader. Check if the previously opened YdbDataReader has  been closed.");
        }

        try
        {
            executeQuerySettings.RpcErrorHandler = e => { Session.OnStatus(e.Status.ConvertStatus()); };

            await using var streamIterator = Session.ExecuteQuery(query, parameters, executeQuerySettings,
                ydbTransaction?.TransactionControl); // closing grpc stream when will close this IAsyncEnumerator

            await foreach (var part in streamIterator)
            {
                var status = Status.FromProto(part.Status, part.Issues);

                Session.OnStatus(status);

                status.EnsureSuccess();

                if (ydbTransaction != null)
                {
                    ydbTransaction.TxId ??= part.TxMeta.Id;
                }

                yield return (part.ResultSetIndex, part.ResultSet);
            }
        }
        finally
        {
            CurrentReader = null;
        }
    }

    private void EnsureConnectionOpen()
    {
        if (ConnectionState == ConnectionState.Closed)
        {
            throw new InvalidOperationException("The connection is closed.");
        }
    }
}
