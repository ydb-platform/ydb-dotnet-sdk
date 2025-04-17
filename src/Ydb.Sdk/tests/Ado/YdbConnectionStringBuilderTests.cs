using Xunit;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tests.Ado;

public class YdbConnectionStringBuilderTests
{
    [Fact]
    public void InitDefaultValues_WhenEmptyConstructorInvoke_ReturnDefaultConnectionString()
    {
        var connectionString = new YdbConnectionStringBuilder();

        Assert.Equal(2136, connectionString.Port);
        Assert.Equal("localhost", connectionString.Host);
        Assert.Equal("/local", connectionString.Database);
        Assert.Equal(100, connectionString.MaxSessionPool);
        Assert.Null(connectionString.User);
        Assert.Null(connectionString.Password);
        Assert.Equal(10, connectionString.KeepAlivePingDelay);
        Assert.Equal(10, connectionString.KeepAlivePingTimeout);
        Assert.Equal("", connectionString.ConnectionString);
    }

    [Fact]
    public void InitConnectionStringBuilder_WhenUnexpectedKey_ThrowException()
    {
        Assert.Equal("Key doesn't support: unexpectedkey", Assert.Throws<ArgumentException>(() =>
            new YdbConnectionStringBuilder("UnexpectedKey=123;Port=2135;")).Message);

        Assert.Equal("Key doesn't support: unexpectedkey", Assert.Throws<ArgumentException>(() =>
            new YdbConnectionStringBuilder { ConnectionString = "UnexpectedKey=123;Port=2135;" }).Message);
    }

    [Fact]
    public void InitConnectionStringBuilder_WhenExpectedKeys_ReturnUpdatedConnectionString()
    {
        var connectionString =
            new YdbConnectionStringBuilder("Host=server;Port=2135;Database=/my/path;User=Kirill;UseTls=true;" +
                                           "KeepAlivePingDelay=30;KeepAlivePingTimeout=60");

        Assert.Equal(2135, connectionString.Port);
        Assert.Equal("server", connectionString.Host);
        Assert.Equal("/my/path", connectionString.Database);
        Assert.Equal(100, connectionString.MaxSessionPool);
        Assert.Equal("Kirill", connectionString.User);
        Assert.Equal(30, connectionString.KeepAlivePingDelay);
        Assert.Equal(60, connectionString.KeepAlivePingTimeout);
        Assert.Null(connectionString.Password);
        Assert.Equal("Host=server;Port=2135;Database=/my/path;User=Kirill;UseTls=True;" +
                     "KeepAlivePingDelay=30;KeepAlivePingTimeout=60", connectionString.ConnectionString);
    }

    [Fact]
    public void Host_WhenSetInProperty_ReturnUpdatedConnectionString()
    {
        var connectionString = new YdbConnectionStringBuilder("Host=server;Port=2135;Database=/my/path;User=Kirill");

        Assert.Equal("server", connectionString.Host);
        connectionString.Host = "new_server";
        Assert.Equal("new_server", connectionString.Host);

        Assert.Equal("Host=new_server;Port=2135;Database=/my/path;User=Kirill",
            connectionString.ConnectionString);
    }
}
