using Xunit;
using Ydb.Sdk.Auth;

namespace Ydb.Sdk.Ado.Tests;

public class YdbConnectionStringBuilderTests
{
    private const int MessageSize = 64 * 1024 * 1024;

    [Fact]
    public void InitDefaultValues_WhenEmptyConstructorInvoke_ReturnDefaultConnectionString()
    {
        var ydbConnectionStringBuilder = new YdbConnectionStringBuilder();

        Assert.Equal(2136, ydbConnectionStringBuilder.Port);
        Assert.Equal("localhost", ydbConnectionStringBuilder.Host);
        Assert.Equal("/local", ydbConnectionStringBuilder.Database);
        Assert.Equal(0, ydbConnectionStringBuilder.MinPoolSize);
        Assert.Equal(100, ydbConnectionStringBuilder.MaxPoolSize);
        Assert.Equal(5, ydbConnectionStringBuilder.CreateSessionTimeout);
        Assert.Equal(300, ydbConnectionStringBuilder.SessionIdleTimeout);
        Assert.Null(ydbConnectionStringBuilder.User);
        Assert.Null(ydbConnectionStringBuilder.Password);
        Assert.Equal(5, ydbConnectionStringBuilder.ConnectTimeout);
        Assert.Equal(10, ydbConnectionStringBuilder.KeepAlivePingDelay);
        Assert.Equal(10, ydbConnectionStringBuilder.KeepAlivePingTimeout);
        Assert.Equal("", ydbConnectionStringBuilder.ConnectionString);
        Assert.False(ydbConnectionStringBuilder.EnableMultipleHttp2Connections);
        Assert.Equal(MessageSize, ydbConnectionStringBuilder.MaxSendMessageSize);
        Assert.Equal(MessageSize, ydbConnectionStringBuilder.MaxReceiveMessageSize);
        Assert.False(ydbConnectionStringBuilder.DisableDiscovery);
        Assert.False(ydbConnectionStringBuilder.DisableServerBalancer);
        Assert.False(ydbConnectionStringBuilder.UseTls);
        Assert.False(ydbConnectionStringBuilder.EnableImplicitSession);
        Assert.Null(ydbConnectionStringBuilder.ServiceAccountKeyFilePath);
        Assert.False(ydbConnectionStringBuilder.EnableMetadataCredentials);

        Assert.Equal("UseTls=False;Host=localhost;Port=2136;Database=/local;User=;Password=;ConnectTimeout=5;" +
                     "KeepAlivePingDelay=10;KeepAlivePingTimeout=10;EnableMultipleHttp2Connections=False;" +
                     $"MaxSendMessageSize={MessageSize};MaxReceiveMessageSize={MessageSize};DisableDiscovery=False;" +
                     "ServiceAccountKeyFilePath=;EnableMetadataCredentials=False",
            ydbConnectionStringBuilder.GrpcConnectionString);
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
        var ydbConnectionStringBuilder = new YdbConnectionStringBuilder(
            "Host=server;Port=2135;Database=/my/path;User=Kirill;UseTls=true;MinPoolSize=10;MaxPoolSize=50;" +
            "CreateSessionTimeout=30;SessionIdleTimeout=600;ConnectTimeout=30;KeepAlivePingDelay=30;" +
            "KeepAlivePingTimeout=60;EnableMultipleHttp2Connections=true;MaxSendMessageSize=1000000;" +
            "MaxReceiveMessageSize=1000000;DisableDiscovery=true;DisableServerBalancer=true;EnableImplicitSession=true;"
        );

        Assert.Equal(2135, ydbConnectionStringBuilder.Port);
        Assert.Equal("server", ydbConnectionStringBuilder.Host);
        Assert.Equal("/my/path", ydbConnectionStringBuilder.Database);
        Assert.Equal(10, ydbConnectionStringBuilder.MinPoolSize);
        Assert.Equal(50, ydbConnectionStringBuilder.MaxPoolSize);
        Assert.Equal(30, ydbConnectionStringBuilder.CreateSessionTimeout);
        Assert.Equal(600, ydbConnectionStringBuilder.SessionIdleTimeout);
        Assert.Equal("Kirill", ydbConnectionStringBuilder.User);
        Assert.Equal(30, ydbConnectionStringBuilder.ConnectTimeout);
        Assert.Equal(30, ydbConnectionStringBuilder.KeepAlivePingDelay);
        Assert.Equal(60, ydbConnectionStringBuilder.KeepAlivePingTimeout);
        Assert.Null(ydbConnectionStringBuilder.Password);
        Assert.True(ydbConnectionStringBuilder.EnableMultipleHttp2Connections);
        Assert.Equal(1000000, ydbConnectionStringBuilder.MaxSendMessageSize);
        Assert.Equal(1000000, ydbConnectionStringBuilder.MaxReceiveMessageSize);
        Assert.Equal("Host=server;Port=2135;Database=/my/path;User=Kirill;UseTls=True;" +
                     "MinPoolSize=10;MaxPoolSize=50;CreateSessionTimeout=30;" +
                     "SessionIdleTimeout=600;" +
                     "ConnectTimeout=30;KeepAlivePingDelay=30;KeepAlivePingTimeout=60;" +
                     "EnableMultipleHttp2Connections=True;" +
                     "MaxSendMessageSize=1000000;MaxReceiveMessageSize=1000000;" +
                     "DisableDiscovery=True;DisableServerBalancer=True;EnableImplicitSession=True",
            ydbConnectionStringBuilder.ConnectionString);
        Assert.True(ydbConnectionStringBuilder.DisableDiscovery);
        Assert.True(ydbConnectionStringBuilder.DisableServerBalancer);
        Assert.True(ydbConnectionStringBuilder.EnableImplicitSession);
        Assert.Equal("UseTls=True;Host=server;Port=2135;Database=/my/path;User=Kirill;Password=;ConnectTimeout=30;" +
                     "KeepAlivePingDelay=30;KeepAlivePingTimeout=60;EnableMultipleHttp2Connections=True;" +
                     "MaxSendMessageSize=1000000;MaxReceiveMessageSize=1000000;DisableDiscovery=True;" +
                     "ServiceAccountKeyFilePath=;EnableMetadataCredentials=False",
            ydbConnectionStringBuilder.GrpcConnectionString);
    }

    [Fact]
    public void Host_WhenSetInProperty_ReturnUpdatedConnectionString()
    {
        var ydbConnectionStringBuilder = new YdbConnectionStringBuilder(
            "Host=server;Port=2135;Database=/my/path;ServiceAccountKeyFilePath=./k.json");
        Assert.Equal("UseTls=False;Host=server;Port=2135;Database=/my/path;User=;Password=;ConnectTimeout=5;" +
                     "KeepAlivePingDelay=10;KeepAlivePingTimeout=10;EnableMultipleHttp2Connections=False;" +
                     $"MaxSendMessageSize={MessageSize};MaxReceiveMessageSize={MessageSize};DisableDiscovery=False;" +
                     "ServiceAccountKeyFilePath=./k.json;EnableMetadataCredentials=False",
            ydbConnectionStringBuilder.GrpcConnectionString);
        Assert.Equal("server", ydbConnectionStringBuilder.Host);
        ydbConnectionStringBuilder.Host = "new_server";
        Assert.Equal("new_server", ydbConnectionStringBuilder.Host);
        Assert.Equal("UseTls=False;Host=new_server;Port=2135;Database=/my/path;User=;Password=;ConnectTimeout=5;" +
                     "KeepAlivePingDelay=10;KeepAlivePingTimeout=10;EnableMultipleHttp2Connections=False;" +
                     $"MaxSendMessageSize={MessageSize};MaxReceiveMessageSize={MessageSize};DisableDiscovery=False;" +
                     "ServiceAccountKeyFilePath=./k.json;EnableMetadataCredentials=False",
            ydbConnectionStringBuilder.GrpcConnectionString);
        Assert.Equal("Host=new_server;Port=2135;Database=/my/path;ServiceAccountKeyFilePath=./k.json",
            ydbConnectionStringBuilder.ConnectionString);
    }

    [Fact]
    public void SetProperty_WhenPropertyNeedsTrimOperation_ReturnUpdatedConnectionString()
    {
        var ydbConnectionStringBuilder =
            new YdbConnectionStringBuilder(" Host  =server;Port=2135;   EnableMultipleHttp2Connections  =true");

        Assert.Equal(2135, ydbConnectionStringBuilder.Port);
        Assert.Equal("server", ydbConnectionStringBuilder.Host);
        Assert.True(ydbConnectionStringBuilder.EnableMultipleHttp2Connections);

        Assert.Equal("Host=server;Port=2135;EnableMultipleHttp2Connections=True",
            ydbConnectionStringBuilder.ConnectionString);

        ydbConnectionStringBuilder.EnableMultipleHttp2Connections = false;

        Assert.Equal("Host=server;Port=2135;EnableMultipleHttp2Connections=False",
            ydbConnectionStringBuilder.ConnectionString);
    }

    [Fact]
    public void SetMutuallyExclusiveProperty_Throws_WhenCredentialsProviderIsSet()
    {
        var builder = new YdbConnectionStringBuilder("Host=server;Port=2135;")
        {
            CredentialsProvider = new TokenProvider("TokenProvider")
        };

        var ex = Assert.Throws<ArgumentException>(() => builder.ServiceAccountKeyFilePath = "/path/to/key.json");
        Assert.Equal(nameof(YdbConnectionStringBuilder.ServiceAccountKeyFilePath), ex.ParamName);
        ex = Assert.Throws<ArgumentException>(() => builder.EnableMetadataCredentials = true);
        Assert.Equal(nameof(YdbConnectionStringBuilder.EnableMetadataCredentials), ex.ParamName);
        ex = Assert.Throws<ArgumentException>(() => builder.User = "user");
        Assert.Equal(nameof(YdbConnectionStringBuilder.User), ex.ParamName);
    }

    [Fact]
    public void SetMutuallyExclusiveProperty_Throws_WhenEnableMetadataCredentialsIsSet()
    {
        var builder = new YdbConnectionStringBuilder("EnableMetadataCredentials=true");

        var ex = Assert.Throws<ArgumentException>(() => builder.ServiceAccountKeyFilePath = "/path/to/key.json");
        Assert.Equal(nameof(YdbConnectionStringBuilder.ServiceAccountKeyFilePath), ex.ParamName);
        ex = Assert.Throws<ArgumentException>(() => builder.User = "user");
        Assert.Equal(nameof(YdbConnectionStringBuilder.User), ex.ParamName);
        ex = Assert.Throws<ArgumentException>(() =>
        {
            _ = new YdbConnectionStringBuilder("Host=server;Port=2135;")
            {
                EnableMetadataCredentials = true,
                CredentialsProvider = new TokenProvider("TokenProvider")
            };
        });
        Assert.Equal(nameof(YdbConnectionStringBuilder.CredentialsProvider), ex.ParamName);
    }

    [Fact]
    public void SetMutuallyExclusiveProperty_Throws_WhenUserIsSet()
    {
        var builder = new YdbConnectionStringBuilder("User=user");

        var ex = Assert.Throws<ArgumentException>(() => builder.EnableMetadataCredentials = true);
        Assert.Equal(nameof(YdbConnectionStringBuilder.EnableMetadataCredentials), ex.ParamName);
        ex = Assert.Throws<ArgumentException>(() => builder.ServiceAccountKeyFilePath = "/path/to/key.json");
        Assert.Equal(nameof(YdbConnectionStringBuilder.ServiceAccountKeyFilePath), ex.ParamName);
        ex = Assert.Throws<ArgumentException>(() =>
        {
            _ = new YdbConnectionStringBuilder("User=user")
                { CredentialsProvider = new TokenProvider("TokenProvider") };
        });
        Assert.Equal(nameof(YdbConnectionStringBuilder.CredentialsProvider), ex.ParamName);
    }

    [Fact]
    public void SetMutuallyExclusiveProperty_Throws_WhenServiceAccountKeyFilePathIsSet()
    {
        var builder = new YdbConnectionStringBuilder("ServiceAccountKeyFilePath=/path/to/key.json");

        var ex = Assert.Throws<ArgumentException>(() => builder.EnableMetadataCredentials = true);
        Assert.Equal(nameof(YdbConnectionStringBuilder.EnableMetadataCredentials), ex.ParamName);
        ex = Assert.Throws<ArgumentException>(() => builder.User = "user");
        Assert.Equal(nameof(YdbConnectionStringBuilder.User), ex.ParamName);
        ex = Assert.Throws<ArgumentException>(() =>
        {
            _ = new YdbConnectionStringBuilder("Host=server;Port=2135;")
            {
                ServiceAccountKeyFilePath = "/path/to/key.json",
                CredentialsProvider = new TokenProvider("TokenProvider")
            };
        });
        Assert.Equal(nameof(YdbConnectionStringBuilder.CredentialsProvider), ex.ParamName);
    }
}
