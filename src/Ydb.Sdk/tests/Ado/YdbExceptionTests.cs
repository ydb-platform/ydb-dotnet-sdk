using System.Data;
using Xunit;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tests.Ado;

[Trait("Category", "Integration")]
public class YdbExceptionTests
{
    [Fact]
    public async Task IsTransient_WhenSchemaError_ReturnFalse()
    {
        await using var ydbConnection = new YdbConnection();
        await ydbConnection.OpenAsync();
        var ydbCommand = new YdbCommand(ydbConnection)
        {
            CommandText = "CREATE TABLE A(text Text)"
        }; // Not exists primary key

        Assert.False(Assert.Throws<YdbException>(() => ydbCommand.ExecuteNonQuery()).IsTransient);
    }

    [Fact]
    public async Task IsTransient_WhenAborted_ReturnTrueAndMakeEmptyRollback()
    {
        var bankTable = $"Bank_{Utils.Net}";
        await using var ydbConnection = new YdbConnection();
        await ydbConnection.OpenAsync();
        var ydbCommand = new YdbCommand(ydbConnection)
        {
            CommandText = $"CREATE TABLE {bankTable}(id Int32, amount Int32, PRIMARY KEY (id))"
        }; // Not exists primary key
        await ydbCommand.ExecuteNonQueryAsync();
        ydbCommand.CommandText = $"INSERT INTO {bankTable}(id, amount) VALUES (1, 100)";
        await ydbCommand.ExecuteNonQueryAsync();

        ydbCommand.Transaction = ydbConnection.BeginTransaction();
        ydbCommand.CommandText = $"SELECT amount FROM {bankTable} WHERE id = 1";
        var select = (int)(await ydbCommand.ExecuteScalarAsync())!;
        Assert.Equal(100, select);

        await using var anotherConnection = new YdbConnection();
        await anotherConnection.OpenAsync();
        await new YdbCommand(anotherConnection)
        {
            CommandText = $"UPDATE {bankTable} SET amount = amount + 50 WHERE id = 1"
        }.ExecuteNonQueryAsync();

        ydbCommand.CommandText = $"UPDATE {bankTable} SET amount = amount + $var WHERE id = 1";
        ydbCommand.Parameters.AddWithValue("$var", DbType.Int32, select);
        Assert.True(Assert.Throws<YdbException>(() =>
        {
            ydbCommand.ExecuteNonQuery();
            ydbCommand.Transaction.Commit();
        }).IsTransient);
        await new YdbCommand(anotherConnection)
        {
            CommandText = $"DROP TABLE {bankTable}"
        }.ExecuteNonQueryAsync();
        Assert.Equal("Status: NotFound", Assert.Throws<YdbException>(() => ydbCommand.Transaction.Commit()).Message);

        await ydbCommand.Transaction!.RollbackAsync();
        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            Assert.Throws<InvalidOperationException>(() => ydbCommand.Transaction!.Commit()).Message);
        Assert.Equal("This YdbTransaction has completed; it is no longer usable",
            Assert.Throws<InvalidOperationException>(() => ydbCommand.Transaction!.Rollback()).Message);
    }
}
