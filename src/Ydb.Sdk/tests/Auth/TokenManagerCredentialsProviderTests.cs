using Moq;
using Xunit;
using Ydb.Sdk.Auth;

namespace Ydb.Sdk.Tests.Auth;

[Trait("Category", "Unit")]
public class TokenManagerCredentialsProviderTests
{
    private const string Token = "SomeToken";

    private readonly Mock<IAuthClient> _mockAuthClient = new();
    private readonly Mock<IClock> _mockClock = new();


    [Fact]
    public async Task SyncState_To_ErrorState_To_SyncState_To_ActiveState()
    {
        var now = DateTime.UtcNow;

        _mockAuthClient.SetupSequence(authClient => authClient.FetchToken())
            .ThrowsAsync(new StatusUnsuccessfulException(
                new Status(StatusCode.Unavailable, new List<Issue> { new(":(") }))
            )
            .ReturnsAsync(new TokenResponse(Token, now.Add(TimeSpan.FromSeconds(2))));
        _mockClock.Setup(clock => clock.UtcNow).Returns(now);
        var credentialsProvider = new TokenManagerCredentialsProvider(_mockAuthClient.Object, _mockClock.Object);

        await Assert.ThrowsAsync<StatusUnsuccessfulException>(() => credentialsProvider.GetAuthInfoAsync().AsTask());
        Assert.Equal(Token, await credentialsProvider.GetAuthInfoAsync());
        Assert.Equal(Token, await credentialsProvider.GetAuthInfoAsync());
        Assert.Equal(Token, await credentialsProvider.GetAuthInfoAsync());

        _mockAuthClient.Verify(authClient => authClient.FetchToken(), Times.Exactly(2));
    }

    [Fact]
    public async Task SyncState_To_ActiveState_To_BackgroundState_To_ActiveState()
    {
        var now = DateTime.UtcNow;
        var tcsTokenResponse = new TaskCompletionSource<TokenResponse>();

        _mockAuthClient.SetupSequence(authClient => authClient.FetchToken())
            .ReturnsAsync(new TokenResponse(Token, now.Add(TimeSpan.FromSeconds(3))))
            .ReturnsAsync(new TokenResponse(Token + Token, now.Add(TimeSpan.FromSeconds(4))))
            .Returns(new ValueTask<TokenResponse>(tcsTokenResponse.Task));
        _mockClock.SetupSequence(clock => clock.UtcNow)
            .Returns(now)
            .Returns(now.Add(TimeSpan.FromSeconds(2)))
            .Returns(now.Add(TimeSpan.FromSeconds(3)))
            .Returns(now.Add(TimeSpan.FromSeconds(3)))
            .Returns(now.Add(TimeSpan.FromSeconds(3)))
            .Returns(now.Add(TimeSpan.FromSeconds(3)))
            .Returns(now.Add(TimeSpan.FromSeconds(4)));
        var credentialsProvider = new TokenManagerCredentialsProvider(_mockAuthClient.Object, _mockClock.Object);

        Assert.Equal(Token, await credentialsProvider.GetAuthInfoAsync());
        Assert.Equal(Token, await credentialsProvider.GetAuthInfoAsync());
        Assert.Equal(Token + Token, await credentialsProvider.GetAuthInfoAsync());
        Assert.Equal(Token + Token, await credentialsProvider.GetAuthInfoAsync());
        Assert.Equal(Token + Token, await credentialsProvider.GetAuthInfoAsync());
        tcsTokenResponse.SetResult(new TokenResponse(Token + "3", now.Add(TimeSpan.FromSeconds(10))));
        Assert.Equal(Token + "3", await credentialsProvider.GetAuthInfoAsync());
        Assert.Equal(Token + "3", await credentialsProvider.GetAuthInfoAsync());

        _mockAuthClient.Verify(authClient => authClient.FetchToken(), Times.Exactly(3));
    }

    [Fact]
    public async Task SyncState_To_ActiveState_To_SyncState_To_ActiveState()
    {
        var now = DateTime.UtcNow;

        _mockAuthClient.SetupSequence(authClient => authClient.FetchToken())
            .ReturnsAsync(new TokenResponse(Token, now.Add(TimeSpan.FromSeconds(2))))
            .ReturnsAsync(new TokenResponse(Token + "1", now.Add(TimeSpan.FromSeconds(10))));
        _mockClock.SetupSequence(clock => clock.UtcNow)
            .Returns(now)
            .Returns(now.Add(TimeSpan.FromSeconds(4)))
            .Returns(now.Add(TimeSpan.FromSeconds(4)));
        var credentialsProvider = new TokenManagerCredentialsProvider(_mockAuthClient.Object, _mockClock.Object);

        Assert.Equal(Token, await credentialsProvider.GetAuthInfoAsync());
        Assert.Equal(Token + "1", await credentialsProvider.GetAuthInfoAsync());
        Assert.Equal(Token + "1", await credentialsProvider.GetAuthInfoAsync());
        _mockAuthClient.Verify(authClient => authClient.FetchToken(), Times.Exactly(2));
    }
}
