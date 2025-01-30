using System.Data;
using Xunit;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Tests.Ado.Specification;
using Ydb.Sdk.Tests.Fixture;

namespace Ydb.Sdk.Tests.Ado;

public class YdbTransactionTests : YdbAdoNetFixture
{
    private static readonly TemporaryTables<YdbTransactionTests> Tables = new();

    public YdbTransactionTests(YdbFactoryFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public void Rollback_WhenUpsertThenRollback_ReturnPrevRow()
    {
        using var connection = CreateOpenConnection();

        var ydbTransaction = connection.BeginTransaction();
        var ydbCommand = connection.CreateCommand();
        ydbCommand.Transaction = ydbTransaction;

        ydbCommand.CommandText = $@"
                                UPSERT INTO {Tables.Seasons} (series_id, season_id, first_aired) 
                                VALUES (@series_id, @season_id, @air_date);
                                ";
        ydbCommand.Parameters.Add(new YdbParameter
            { ParameterName = "$series_id", DbType = DbType.UInt64, Value = 1U });
        ydbCommand.Parameters.Add(new YdbParameter
            { ParameterName = "$season_id", DbType = DbType.UInt64, Value = 3U });
        ydbCommand.Parameters.Add(new YdbParameter
            { ParameterName = "$air_date", DbType = DbType.Date, Value = new DateTime(2022, 2, 24) });
        ydbCommand.ExecuteNonQuery();

        ydbTransaction.Rollback();

        ydbCommand.CommandText = $"SELECT first_aired FROM {Tables.Seasons} WHERE series_id = 1 AND season_id = 3";
        Assert.Equal(new DateTime(2008, 11, 21), ydbCommand.ExecuteScalar());
    }

    [Fact]
    public void Commit_WhenMakeTwoUpsertOperation_ReturnUpdatedTables()
    {
        using var connection = CreateOpenConnection();

        var ydbTransaction = connection.BeginTransaction(IsolationLevel.Serializable);
        var ydbCommand = connection.CreateCommand();

        ydbCommand.CommandText = $@"
                                INSERT INTO {Tables.Seasons} (series_id, season_id, title, first_aired, last_aired) 
                                VALUES (2, 6, ""Season6"", Date(""2006-02-03""), Date(""2006-03-03""));
                                ";
        ydbCommand.ExecuteNonQuery();
        ydbCommand.CommandText = $@"
                                INSERT INTO {Tables.Episodes} (series_id, season_id, episode_id, title, air_date)
                                VALUES (2, 6, 1, ""Yesterday's Jam"", Date(""2006-02-03""))
                                ";
        ydbCommand.ExecuteNonQuery();
        ydbTransaction.Commit();

        ydbTransaction = connection.BeginTransaction(TxMode.SnapshotRo);
        ydbCommand.Transaction = ydbTransaction;
        ydbCommand.CommandText = $"SELECT title FROM {Tables.Seasons} WHERE series_id = 2 AND season_id = 6";
        var dbDataReader = ydbCommand.ExecuteReader();

        Assert.True(dbDataReader.Read());
        Assert.Equal("Season6", dbDataReader.GetString(0));
        Assert.False(dbDataReader.Read());

        ydbCommand.CommandText =
            $"SELECT title FROM {Tables.Episodes} WHERE series_id = 2 AND season_id = 6 AND episode_id = 1";
        dbDataReader = ydbCommand.ExecuteReader();

        Assert.True(dbDataReader.Read());
        Assert.Equal("Yesterday's Jam", dbDataReader.GetString(0));
        Assert.False(dbDataReader.Read());

        ydbTransaction.Commit();
    }

    [Fact]
    public void Commit_WhenEmptyYdbCommand_DoNothing()
    {
        using var connection = CreateOpenConnection();

        var ydbTransaction = connection.BeginTransaction();
        var ydbCommand = connection.CreateCommand();
        ydbCommand.Transaction = ydbTransaction;
        ydbTransaction.Commit(); // Do nothing

        // Out transaction executing
        ydbCommand.CommandText = $"SELECT first_aired FROM {Tables.Seasons} WHERE series_id = 1 AND season_id = 3";
        Assert.Equal(new DateTime(2008, 11, 21), ydbCommand.ExecuteScalar());
    }

    [Fact]
    public void CommitAndRollback_WhenDoubleCommit_ThrowException()
    {
        using var connection = CreateOpenConnection();

        var ydbTransaction = connection.BeginTransaction();

        ydbTransaction.Commit(); // Do nothing
        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            Assert.Throws<InvalidOperationException>(() => ydbTransaction.Commit()).Message);
        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            Assert.Throws<InvalidOperationException>(() => ydbTransaction.Rollback()).Message);

        ydbTransaction = connection.BeginTransaction();
        var ydbCommand = connection.CreateCommand();
        ydbCommand.CommandText = "SELECT 1";
        Assert.Equal(1, ydbCommand.ExecuteScalar());
        ydbTransaction.Commit();
        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            Assert.Throws<InvalidOperationException>(() => ydbTransaction.Commit()).Message);
        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            Assert.Throws<InvalidOperationException>(() => ydbTransaction.Rollback()).Message);
    }

    [Fact]
    public void BeginTransaction_WhenYdbDataReaderIsClosed_ThrowExceptionTransactionIsBroken()
    {
        using var connection = CreateOpenConnection();

        using var ydbTransaction = connection.BeginTransaction();
        var ydbCommand = connection.CreateCommand();
        ydbCommand.CommandText = "SELECT 1; SELECT 2; SELECT 3";
        var dbDataReader = ydbCommand.ExecuteReader();
        dbDataReader.Read();
        Assert.Equal("YdbDataReader was closed during transaction execution. Transaction is broken!",
            Assert.Throws<YdbException>(() => dbDataReader.Close()).Message);
        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            Assert.Throws<InvalidOperationException>(() => ydbTransaction.Commit()).Message);
        ydbTransaction.Rollback();
        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            Assert.Throws<InvalidOperationException>(() => ydbTransaction.Commit()).Message);
        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            Assert.Throws<InvalidOperationException>(() => ydbTransaction.Rollback()).Message);
    }

    [Fact]
    public async Task BeginTransaction_WhenTxIdIsReceivedThenYdbDataReaderIsClosed_SuccessCommit()
    {
        await using var connection = await CreateOpenConnectionAsync();

        var tx = connection.BeginTransaction();
        var ydbCommand1 = connection.CreateCommand();
        ydbCommand1.CommandText = "SELECT 1";
        Assert.Equal(1, await ydbCommand1.ExecuteScalarAsync());
        var ydbCommand2 = connection.CreateCommand();
        ydbCommand2.CommandText = "SELECT 1; SELECT 2; SELECT 3";
        var dbDataReader = await ydbCommand2.ExecuteReaderAsync();
        await dbDataReader.NextResultAsync();
        Assert.Equal("YdbDataReader was closed during transaction execution. Transaction is broken!",
            (await Assert.ThrowsAsync<YdbException>(() => dbDataReader.CloseAsync())).Message);
        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            (await Assert.ThrowsAsync<InvalidOperationException>(() => tx.CommitAsync())).Message);
        await tx.RollbackAsync(); // do nothing transaction
        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            (await Assert.ThrowsAsync<InvalidOperationException>(() => tx.CommitAsync())).Message);
        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            (await Assert.ThrowsAsync<InvalidOperationException>(() => tx.RollbackAsync())).Message);
    }

    [Fact]
    public void CommitAndRollback_WhenStreamIsOpened_ThrowException()
    {
        using var connection = CreateOpenConnection();

        var ydbTransaction = connection.BeginTransaction();
        var ydbCommand = connection.CreateCommand();

        ydbCommand.CommandText = "SELECT 1; SELECT 2; SELECT 3";
        var dbDataReader = ydbCommand.ExecuteReader(); // Open stream

        Assert.Equal("A command is already in progress: SELECT 1; SELECT 2; SELECT 3",
            Assert.Throws<YdbOperationInProgressException>(() => ydbTransaction.Commit()).Message);
        Assert.Equal("A command is already in progress: SELECT 1; SELECT 2; SELECT 3",
            Assert.Throws<YdbOperationInProgressException>(() => ydbTransaction.Rollback()).Message);

        Assert.True(dbDataReader.NextResult());
        Assert.True(dbDataReader.NextResult());
        Assert.False(dbDataReader.NextResult());

        dbDataReader.Close(); // Close stream
        ydbTransaction.Commit();
        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            Assert.Throws<InvalidOperationException>(() => ydbTransaction.Rollback()).Message);
    }

    [Fact]
    public void CommitAndRollback_WhenConnectionIsClosed_ThrowException()
    {
        using var connection = CreateOpenConnection();

        var ydbTransaction = connection.BeginTransaction();
        var ydbCommand = connection.CreateCommand();
        ydbCommand.Transaction = ydbTransaction;

        ydbCommand.CommandText = "SELECT true;";
        Assert.Equal(true, ydbCommand.ExecuteScalar());

        connection.Close();
        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            Assert.Throws<InvalidOperationException>(() => ydbTransaction.Commit()).Message);
        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            Assert.Throws<InvalidOperationException>(() => ydbTransaction.Rollback()).Message);
    }

    [Fact]
    public void CommitAndRollback_WhenConnectionIsClosedAndTxDoesNotStarted_ThrowException()
    {
        using var connection = CreateOpenConnection();

        var ydbTransaction = connection.BeginTransaction();
        connection.Close();

        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            Assert.Throws<InvalidOperationException>(() => ydbTransaction.Commit()).Message);
        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            Assert.Throws<InvalidOperationException>(() => ydbTransaction.Rollback()).Message);
    }

    [Fact]
    public void CommitAndRollback_WhenTransactionIsFailed_ThrowException()
    {
        using var connection = CreateOpenConnection();

        var ydbCommand = connection.CreateCommand();
        ydbCommand.Transaction = connection.BeginTransaction();
        ydbCommand.Transaction.Failed = true;
        ydbCommand.Transaction.TxId = "no_tx";

        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            Assert.Throws<InvalidOperationException>(() => ydbCommand.Transaction.Commit()).Message);

        ydbCommand.Transaction.Rollback(); // Make completed
        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            Assert.Throws<InvalidOperationException>(() => ydbCommand.Transaction.Commit()).Message);
        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            Assert.Throws<InvalidOperationException>(() => ydbCommand.Transaction.Rollback()).Message);
    }

    protected override async Task OnInitializeAsync()
    {
        await using var connection = await CreateOpenConnectionAsync();
        var ydbCommand = connection.CreateCommand();
        ydbCommand.CommandText = Tables.CreateTables;
        await ydbCommand.ExecuteNonQueryAsync();
        ydbCommand.CommandText = Tables.UpsertData;
        await ydbCommand.ExecuteNonQueryAsync();
    }

    protected override async Task OnDisposeAsync()
    {
        await using var connection = await CreateOpenConnectionAsync();
        var ydbCommand = connection.CreateCommand();
        ydbCommand.CommandText = Tables.DeleteTables;
        await ydbCommand.ExecuteNonQueryAsync();
    }
}
