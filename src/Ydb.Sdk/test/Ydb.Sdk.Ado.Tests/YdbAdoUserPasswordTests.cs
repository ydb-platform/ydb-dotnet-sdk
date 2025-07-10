using Xunit;

namespace Ydb.Sdk.Ado.Tests;

public class YdbAdoUserPasswordTests : TestBase
{
    private const string User = "kurdyukovkirya";

    [Fact]
    public async Task Authentication_WhenUserAndPassword_ReturnValidConnection()
    {
        await using var userPasswordConnection =
            new YdbConnection($"{ConnectionString};User={User};Password=password;");
        await userPasswordConnection.OpenAsync();
        Assert.Equal(3, await new YdbCommand(userPasswordConnection)
            { CommandText = "SELECT 1 + 2" }.ExecuteScalarAsync());
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_WhenCreateUser_ReturnEmptyResultSet()
    {
        await using var connection = await CreateOpenConnectionAsync();
        var dbCommand = connection.CreateCommand();
        var user = "user" + Random.Shared.Next();
        dbCommand.CommandText = $"CREATE USER {user} PASSWORD '123qweqwe'";
        await dbCommand.ExecuteNonQueryAsync();
        dbCommand.CommandText = $"DROP USER {user};";
        await dbCommand.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task DisableDiscovery_WhenUserIsCreatedAndPropertyIsTrue_SimpleWorking()
    {
        await using var userPasswordConnection = new YdbConnection(
            $"{ConnectionString};User={User};Password=password;DisableDiscovery=true");
        await userPasswordConnection.OpenAsync();
        var ydbCommand = userPasswordConnection.CreateCommand();
        ydbCommand.CommandText = "SELECT 1 + 2";
        Assert.Equal(3, await ydbCommand.ExecuteScalarAsync());
    }

    protected override async Task OnInitializeAsync()
    {
        await using var connection = await CreateOpenConnectionAsync();
        var ydbCommand = connection.CreateCommand();
        ydbCommand.CommandText = $"CREATE USER {User} PASSWORD 'password'";
        await ydbCommand.ExecuteNonQueryAsync();
    }

    protected override async Task OnDisposeAsync()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        var ydbCommand = ydbConnection.CreateCommand();
        ydbCommand.CommandText = $"DROP USER {User};";
        await ydbCommand.ExecuteNonQueryAsync();
    }
}
