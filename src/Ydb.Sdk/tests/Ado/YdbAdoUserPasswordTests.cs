using Xunit;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tests.Ado;

public class YdbAdoUserPasswordTests
{
    [Fact]
    public async Task Authentication_WhenUserAndPassword_ReturnValidConnection()
    {
        await using var connection = new YdbConnection();
        await connection.OpenAsync();

        var ydbCommand = connection.CreateCommand();
        var kurdyukovkirya = "kurdyukovkirya" + Random.Shared.Next();
        ydbCommand.CommandText = $"CREATE USER {kurdyukovkirya} PASSWORD 'password'";
        await ydbCommand.ExecuteNonQueryAsync();
        await connection.CloseAsync();

        await using var userPasswordConnection = new YdbConnection($"User={kurdyukovkirya};Password=password;");
        await userPasswordConnection.OpenAsync();
        ydbCommand = userPasswordConnection.CreateCommand();
        ydbCommand.CommandText = "SELECT 1 + 2";
        Assert.Equal(3, await ydbCommand.ExecuteScalarAsync());

        await using var newConnection = new YdbConnection();
        await newConnection.OpenAsync();
        ydbCommand = newConnection.CreateCommand();
        ydbCommand.CommandText = $"DROP USER {kurdyukovkirya};";
        await ydbCommand.ExecuteNonQueryAsync();
    }
}
