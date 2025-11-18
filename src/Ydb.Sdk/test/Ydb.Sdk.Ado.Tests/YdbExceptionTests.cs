using System.Data;
using Xunit;

namespace Ydb.Sdk.Ado.Tests;

public class YdbExceptionTests : TestBase
{
    [Fact]
    public async Task IsTransient_WhenSchemaError_ReturnFalse()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        Assert.False(Assert.Throws<YdbException>(() => new YdbCommand("CREATE TABLE A(text Text)", ydbConnection)
            .ExecuteNonQuery()).IsTransient); // Not exists primary key
    }

    [Fact]
    public Task IsTransient_WhenAborted_ReturnTrueAndMakeEmptyRollback() => RunTestWithTemporaryTable(
        "CREATE TABLE `{0}`(id Int32, amount Int32, PRIMARY KEY (id))", $"BankTable_{Guid.NewGuid()}",
        async (ydbConnection, tableName) =>
        {
            var ydbCommand = new YdbCommand($"INSERT INTO {tableName}(id, amount) VALUES (1, 100)", ydbConnection);
            await ydbCommand.ExecuteNonQueryAsync();

            ydbCommand.Transaction = ydbConnection.BeginTransaction();
            ydbCommand.CommandText = $"SELECT amount FROM {tableName} WHERE id = 1";
            var select = (int)(await ydbCommand.ExecuteScalarAsync())!;
            Assert.Equal(100, select);

            await using var anotherConnection = await CreateOpenConnectionAsync();
            await new YdbCommand(anotherConnection)
                    { CommandText = $"UPDATE {tableName} SET amount = amount + 50 WHERE id = 1" }
                .ExecuteNonQueryAsync();

            ydbCommand.CommandText = $"UPDATE {tableName} SET amount = amount + @var WHERE id = 1";
            ydbCommand.Parameters.AddWithValue("var", DbType.Int32, select);
            Assert.True((await Assert.ThrowsAsync<YdbException>(async () =>
            {
                await ydbCommand.ExecuteNonQueryAsync();
                await ydbCommand.Transaction.CommitAsync();
            })).IsTransient);

            Assert.Equal("This YdbTransaction has completed; it is no longer usable",
                Assert.Throws<InvalidOperationException>(() => ydbCommand.Transaction.Commit()).Message);

            await ydbCommand.Transaction.RollbackAsync();
            Assert.Equal("This YdbTransaction has completed; it is no longer usable",
                Assert.Throws<InvalidOperationException>(() => ydbCommand.Transaction!.Commit()).Message);
            Assert.Equal("This YdbTransaction has completed; it is no longer usable",
                Assert.Throws<InvalidOperationException>(() => ydbCommand.Transaction!.Rollback()).Message);
        });
}
