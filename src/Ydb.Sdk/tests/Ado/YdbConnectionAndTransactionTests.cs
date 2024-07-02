using System.Data;
using Xunit;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tests.Ado;

[Collection("Integration YdbConnection tests")]
[CollectionDefinition("Integration YdbConnection isolation tests", DisableParallelization = true)]
[Trait("Category", "Integration")]
public class YdbConnectionAndTransactionTests : IAsyncLifetime
{
    [Fact]
    public void Transaction_WhenUpsertThenRollback_ReturnPrevRow()
    {
        using var connection = new YdbConnection();
        connection.Open();

        var ydbTransaction = connection.BeginTransaction();
        var dbCommand = connection.CreateCommand();
        dbCommand.Transaction = ydbTransaction;

        dbCommand.CommandText = @"
                                UPSERT INTO seasons (series_id, season_id, first_aired) 
                                VALUES ($series_id, $season_id, $air_date);
                                ";
        dbCommand.Parameters.Add(new YdbParameter { ParameterName = "$series_id", DbType = DbType.UInt64, Value = 1u });
        dbCommand.Parameters.Add(new YdbParameter { ParameterName = "$series_id", DbType = DbType.UInt64, Value = 3u });
        dbCommand.Parameters.Add(new YdbParameter
        {
            ParameterName = "$series_id", DbType = DbType.Date,
            Value = new DateTime(2022, 2, 24)
        });
        dbCommand.ExecuteNonQuery();

        ydbTransaction.Rollback();

        dbCommand.CommandText = "SELECT first_aired FROM seasons WHERE series_id = 1 AND season_id = 3";
        Assert.Equal(new DateTime(2008, 11, 21), dbCommand.ExecuteScalar());
    }

    public async Task InitializeAsync()
    {
        await using var connection = new YdbConnection();
        await connection.OpenAsync();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = Utils.CreateTables;
        await dbCommand.ExecuteNonQueryAsync();
        dbCommand.CommandText = Utils.UpsertData;
        await dbCommand.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync()
    {
        await using var connection = new YdbConnection();
        await connection.OpenAsync();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = Utils.DeleteTables;
        await dbCommand.ExecuteNonQueryAsync();
    }
}
