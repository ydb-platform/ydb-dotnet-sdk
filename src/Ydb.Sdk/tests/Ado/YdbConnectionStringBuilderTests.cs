using Xunit;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tests.Ado;

public class YdbConnectionStringBuilderTests
{
    [Fact]
    public void InitDefaultValues_WhenEmptyConstructorInvoke_ReturnDefaultConnectionString()
    {
        var ydbConnectionStringBuilder = new YdbConnectionStringBuilder();

        Assert.Equal(2136, ydbConnectionStringBuilder.Port);
        Assert.Equal("localhost", ydbConnectionStringBuilder.Host);
        Assert.Equal("/local", ydbConnectionStringBuilder.Database);
        Assert.Equal(100, ydbConnectionStringBuilder.MaxSessionPool);
        Assert.Null(ydbConnectionStringBuilder.User);
        Assert.Null(ydbConnectionStringBuilder.Password);
        Assert.Equal(5, ydbConnectionStringBuilder.ConnectTimeout);
        Assert.Equal(10, ydbConnectionStringBuilder.KeepAlivePingDelay);
        Assert.Equal(10, ydbConnectionStringBuilder.KeepAlivePingTimeout);
        Assert.Equal("", ydbConnectionStringBuilder.ConnectionString);
        Assert.False(ydbConnectionStringBuilder.EnableMultipleHttp2Connections);
        Assert.Equal(64 * 1024 * 1024, ydbConnectionStringBuilder.MaxSendMessageSize);
        Assert.Equal(64 * 1024 * 1024, ydbConnectionStringBuilder.MaxReceiveMessageSize);
        Assert.False(ydbConnectionStringBuilder.DisableDiscovery);
        Assert.Equal(5, ydbConnectionStringBuilder.CreateSessionTimeout);
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
        var connectionString = new YdbConnectionStringBuilder(
            "Host=server;Port=2135;Database=/my/path;User=Kirill;UseTls=true;" +
            "ConnectTimeout=30;KeepAlivePingDelay=30;KeepAlivePingTimeout=60;" +
            "EnableMultipleHttp2Connections=true;CreateSessionTimeout=30;" +
            "MaxSendMessageSize=1000000;MaxReceiveMessageSize=1000000;" +
            "DisableDiscovery=true"
        );

        Assert.Equal(2135, connectionString.Port);
        Assert.Equal("server", connectionString.Host);
        Assert.Equal("/my/path", connectionString.Database);
        Assert.Equal(100, connectionString.MaxSessionPool);
        Assert.Equal("Kirill", connectionString.User);
        Assert.Equal(30, connectionString.ConnectTimeout);
        Assert.Equal(30, connectionString.KeepAlivePingDelay);
        Assert.Equal(60, connectionString.KeepAlivePingTimeout);
        Assert.Null(connectionString.Password);
        Assert.True(connectionString.EnableMultipleHttp2Connections);
        Assert.Equal(1000000, connectionString.MaxSendMessageSize);
        Assert.Equal(1000000, connectionString.MaxReceiveMessageSize);
        Assert.Equal("Host=server;Port=2135;Database=/my/path;User=Kirill;UseTls=True;" +
                     "ConnectTimeout=30;KeepAlivePingDelay=30;KeepAlivePingTimeout=60;" +
                     "EnableMultipleHttp2Connections=True;CreateSessionTimeout=30;" +
                     "MaxSendMessageSize=1000000;MaxReceiveMessageSize=1000000;" +
                     "DisableDiscovery=True", connectionString.ConnectionString);
        Assert.True(connectionString.DisableDiscovery);
        Assert.Equal(30, connectionString.CreateSessionTimeout);
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

    [Fact]
    public void SetProperty_WhenPropertyNeedsTrimOperation_ReturnUpdatedConnectionString()
    {
        var connectionString =
            new YdbConnectionStringBuilder(" Host  =server;Port=2135;   EnableMultipleHttp2Connections  =true");

        Assert.Equal(2135, connectionString.Port);
        Assert.Equal("server", connectionString.Host);
        Assert.True(connectionString.EnableMultipleHttp2Connections);

        Assert.Equal("Host=server;Port=2135;EnableMultipleHttp2Connections=True", connectionString.ConnectionString);

        connectionString.EnableMultipleHttp2Connections = false;

        Assert.Equal("Host=server;Port=2135;EnableMultipleHttp2Connections=False", connectionString.ConnectionString);
    }
}
