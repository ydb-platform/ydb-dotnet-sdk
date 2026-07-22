using System.Data;
using Dapper;
using Xunit;
using Ydb.Sdk.Ado.Tests;
using Ydb.Sdk.Ado.Tests.Utils;

namespace Ydb.Sdk.Ado.Dapper.Tests;

public class DapperAutoCommitTests : TestBase
{
    private static readonly TemporaryTables<DapperAutoCommitTests> Tables = new();

    static DapperAutoCommitTests()
    {
        SqlMapper.AddTypeMap(typeof(DateOnly), DbType.Date);
    }

    [Fact]
    public async Task EnableAutoCommit_InTransaction_CommitsOnExecute()
    {
        await using var connection = await CreateOpenConnectionAsync();
        await using var transaction = connection.BeginTransaction();
        connection.EnableAutoCommit();

        await connection.ExecuteAsync(
            $"""
             UPSERT INTO {Tables.Episodes} (series_id, season_id, episode_id, title, air_date)
             VALUES (@series_id, @season_id, @episode_id, @title, @air_date);
             """,
            new
            {
                series_id = 2UL,
                season_id = 5UL,
                episode_id = 200UL,
                title = "Dapper AutoCommit",
                air_date = new DateOnly(2024, 9, 1)
            },
            transaction);

        await using (var other = await CreateOpenConnectionAsync())
        {
            Assert.Equal("Dapper AutoCommit", await other.ExecuteScalarAsync<string>(
                $"""
                 SELECT title FROM {Tables.Episodes}
                 WHERE series_id = 2 AND season_id = 5 AND episode_id = 200
                 """));
        }

        await transaction.CommitAsync(); // no-op after commit_tx
    }

    [Fact]
    public async Task ExecuteInTransaction_WithEnableAutoCommit_Works()
    {
        await using var dataSource = new YdbDataSource(ConnectionString);

        await dataSource.ExecuteInTransactionAsync(async connection =>
        {
            connection.EnableAutoCommit();
            await connection.ExecuteAsync(
                $"""
                 UPSERT INTO {Tables.Episodes} (series_id, season_id, episode_id, title, air_date)
                 VALUES (@series_id, @season_id, @episode_id, @title, @air_date);
                 """,
                new
                {
                    series_id = 2UL,
                    season_id = 5UL,
                    episode_id = 201UL,
                    title = "Dapper EIT AutoCommit",
                    air_date = new DateOnly(2024, 9, 2)
                });
        });

        await using var verify = await CreateOpenConnectionAsync();
        Assert.Equal("Dapper EIT AutoCommit", await verify.ExecuteScalarAsync<string>(
            $"""
             SELECT title FROM {Tables.Episodes}
             WHERE series_id = 2 AND season_id = 5 AND episode_id = 201
             """));
    }

    [Fact]
    public async Task ExecuteInTransaction_MultiStatement_WithoutAutoCommit_Works()
    {
        await using var dataSource = new YdbDataSource(ConnectionString);

        await dataSource.ExecuteInTransactionAsync(async connection =>
        {
            await connection.ExecuteAsync(
                $"""
                 UPSERT INTO {Tables.Episodes} (series_id, season_id, episode_id, title, air_date)
                 VALUES (@series_id, @season_id, @episode_id, @title, @air_date);
                 """,
                new
                {
                    series_id = 2UL,
                    season_id = 5UL,
                    episode_id = 202UL,
                    title = "Dapper Multi 1",
                    air_date = new DateOnly(2024, 9, 3)
                });

            await connection.ExecuteAsync(
                $"""
                 UPDATE {Tables.Episodes} SET title = @title
                 WHERE series_id = 2 AND season_id = 5 AND episode_id = 202;
                 """,
                new { title = "Dapper Multi 2" });
        });

        await using var verify = await CreateOpenConnectionAsync();
        Assert.Equal("Dapper Multi 2", await verify.ExecuteScalarAsync<string>(
            $"""
             SELECT title FROM {Tables.Episodes}
             WHERE series_id = 2 AND season_id = 5 AND episode_id = 202
             """));
    }

    [Fact]
    public async Task EnableAutoCommit_OnLastStatement_CommitsInteractiveTx()
    {
        await using var connection = await CreateOpenConnectionAsync();

        await using var transaction = connection.BeginTransaction();
        await connection.ExecuteAsync(
            $"""
             UPSERT INTO {Tables.Episodes} (series_id, season_id, episode_id, title, air_date)
             VALUES (@series_id, @season_id, @episode_id, @title, @air_date);
             """,
            new
            {
                series_id = 2UL,
                season_id = 5UL,
                episode_id = 203UL,
                title = "Dapper Last 1",
                air_date = new DateOnly(2024, 9, 4)
            },
            transaction);

        connection.EnableAutoCommit();
        await connection.ExecuteAsync(
            $"""
             UPSERT INTO {Tables.Episodes} (series_id, season_id, episode_id, title, air_date)
             VALUES (@series_id, @season_id, @episode_id, @title, @air_date);
             """,
            new
            {
                series_id = 2UL,
                season_id = 5UL,
                episode_id = 204UL,
                title = "Dapper Last 2",
                air_date = new DateOnly(2024, 9, 5)
            },
            transaction);

        await using (var other = await CreateOpenConnectionAsync())
        {
            Assert.Equal(2, await other.ExecuteScalarAsync<int>(
                $"""
                 SELECT COUNT(*) FROM {Tables.Episodes}
                 WHERE series_id = 2 AND season_id = 5 AND episode_id IN (203, 204)
                 """));
        }

        await transaction.CommitAsync();
    }

    protected override async Task OnInitializeAsync()
    {
        await using var connection = await CreateOpenConnectionAsync();
        await connection.ExecuteAsync(Tables.CreateTables);
        await connection.ExecuteAsync(Tables.UpsertData);
    }

    protected override async Task OnDisposeAsync()
    {
        await using var connection = await CreateOpenConnectionAsync();
        await connection.ExecuteAsync(Tables.DeleteTables);
    }
}
