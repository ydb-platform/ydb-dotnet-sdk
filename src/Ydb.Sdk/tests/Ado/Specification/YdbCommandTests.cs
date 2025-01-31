using AdoNet.Specification.Tests;
using Xunit;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tests.Ado.Specification;

public class YdbCommandTests : CommandTestBase<YdbFactoryFixture>
{
    public YdbCommandTests(YdbFactoryFixture fixture) : base(fixture)
    {
    }

    public override void ExecuteScalar_returns_string_when_text()
    {
        using var connection = CreateOpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 'test'u;";
        Assert.Equal("test", command.ExecuteScalar());
    }

    public override void ExecuteScalar_returns_first_when_multiple_rows()
    {
        using var connection = CreateOpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 42 AS id UNION SELECT 43 AS id ORDER BY id;";
        Assert.Equal(42, Convert.ToInt32(command.ExecuteScalar()));
    }

#pragma warning disable xUnit1004
    [Fact(Skip = "TODO maybe SqlParser will mark SQL query as a comment, then return a stub YdbDataReader")]
#pragma warning restore xUnit1004
    public override void ExecuteReader_HasRows_is_false_for_comment()
    {
        base.ExecuteReader_HasRows_is_false_for_comment();
    }

#pragma warning disable xUnit1004
    [Fact(Skip = "CommandBehavior don't supported")]
#pragma warning restore xUnit1004
    public override void ExecuteReader_supports_CloseConnection()
    {
        base.ExecuteReader_supports_CloseConnection();
    }

    public override void ExecuteReader_throws_when_transaction_required()
    {
        using var connection = CreateOpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1;";

        using (connection.BeginTransaction())
        {
            Assert.Throws<YdbException>(() =>
            {
                using (command.ExecuteReader())
                {
                }
            });
        }
    }
    
    public override async Task ExecuteReaderAsync_is_canceled()
    {
        await using var connection = CreateOpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1;";
        var task = command.ExecuteReaderAsync(CanceledToken);
        Assert.Equal(StatusCode.Cancelled,
            (await Assert.ThrowsAnyAsync<YdbException>(() => task)).Code);
    }

    public override async Task ExecuteNonQueryAsync_is_canceled()
    {
        await using var connection = CreateOpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1;";
        var task = command.ExecuteNonQueryAsync(CanceledToken);
        Assert.Equal(StatusCode.Cancelled,
            (await Assert.ThrowsAnyAsync<YdbException>(() => task)).Code);
    }

    public override async Task ExecuteScalarAsync_is_canceled()
    {
        await using var connection = CreateOpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1;";
        var task = command.ExecuteScalarAsync(CanceledToken);
        Assert.Equal(StatusCode.Cancelled,
            (await Assert.ThrowsAnyAsync<YdbException>(() => task)).Code);
    }
}
