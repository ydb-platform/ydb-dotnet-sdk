using Xunit;
using Ydb.Query;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Ado.RetryPolicy;
using Ydb.Sdk.Ado.Session;
using Ydb.Sdk.Ado.Tests.Utils;

namespace Ydb.Sdk.Ado.Tests.Session;

public class RetryableSessionTests
{
    [Fact]
    public async Task MoveNextAsync_WhenRetryableStatus_RetriesUpToMaxAttempts_ThenThrows()
    {
        var factory = new MockPoolingSessionFactory(1)
        {
            IsBroken = _ => false,
            ExecuteQuery = _ => new MockAsyncEnumerator<ExecuteQueryResponsePart>(
                new List<ExecuteQueryResponsePart> { new() { Status = StatusIds.Types.StatusCode.BadSession } })
        };

        var retryableSession = new RetryableSession(new PoolingSessionSource<MockPoolingSession>(
                factory,
                new YdbConnectionStringBuilder { MaxPoolSize = 1 }),
            new YdbRetryPolicyExecutor(new YdbRetryPolicy(new YdbRetryPolicyConfig { MaxAttempts = 5 }))
        );

        var inMemoryStream = await retryableSession.ExecuteQuery(
            "SELECT * FROM session",
            new Dictionary<string, TypedValue>(),
            new GrpcRequestSettings(),
            null
        );

        Assert.Equal(StatusCode.BadSession,
            (await Assert.ThrowsAsync<YdbException>(async () => await inMemoryStream.MoveNextAsync())).Code);
        Assert.Equal(5, factory.SessionOpenedCount);
    }

    [Fact]
    public async Task MoveNextAsync_WhenNonRetryable_ThrowsWithoutRetry()
    {
        var factory = new MockPoolingSessionFactory(1)
        {
            IsBroken = _ => false,
            ExecuteQuery = _ => new MockAsyncEnumerator<ExecuteQueryResponsePart>(
                new List<ExecuteQueryResponsePart> { new() { Status = StatusIds.Types.StatusCode.Unauthorized } })
        };

        var retryableSession = new RetryableSession(new PoolingSessionSource<MockPoolingSession>(
                factory,
                new YdbConnectionStringBuilder { MaxPoolSize = 1 }),
            new YdbRetryPolicyExecutor(new YdbRetryPolicy(new YdbRetryPolicyConfig { MaxAttempts = 5 }))
        );

        var inMemoryStream = await retryableSession.ExecuteQuery(
            "SELECT * FROM session",
            new Dictionary<string, TypedValue>(),
            new GrpcRequestSettings(),
            null
        );

        Assert.Equal(StatusCode.Unauthorized,
            (await Assert.ThrowsAsync<YdbException>(async () => await inMemoryStream.MoveNextAsync())).Code);
        Assert.Equal(1, factory.SessionOpenedCount);
    }

    [Fact]
    public async Task MoveNextAsync_SucceedsOnThirdAttempt_StopsRetrying()
    {
        var attempt = 0;
        var factory = new MockPoolingSessionFactory(1)
        {
            IsBroken = _ => false,
            ExecuteQuery = _ =>
            {
                attempt++;
                if (attempt < 3)
                    return new MockAsyncEnumerator<ExecuteQueryResponsePart>(
                        new List<ExecuteQueryResponsePart>
                        {
                            new() { Status = StatusIds.Types.StatusCode.BadSession }
                        });
                return new MockAsyncEnumerator<ExecuteQueryResponsePart>(
                    new List<ExecuteQueryResponsePart> { new() { Status = StatusIds.Types.StatusCode.Success } }
                );
            }
        };

        var retryableSession = new RetryableSession(new PoolingSessionSource<MockPoolingSession>(
                factory,
                new YdbConnectionStringBuilder { MaxPoolSize = 1 }),
            new YdbRetryPolicyExecutor(new YdbRetryPolicy(new YdbRetryPolicyConfig { MaxAttempts = 5 }))
        );
        var inMemoryStream = await retryableSession.ExecuteQuery(
            "SELECT * FROM session",
            new Dictionary<string, TypedValue>(),
            new GrpcRequestSettings(),
            null
        );

        Assert.Throws<InvalidOperationException>(() => inMemoryStream.Current);
        var hasItem = await inMemoryStream.MoveNextAsync();
        Assert.True(hasItem);
        Assert.False(inMemoryStream.Current.Status.IsNotSuccess());
        Assert.False(await inMemoryStream.MoveNextAsync());
        Assert.Throws<InvalidOperationException>(() => inMemoryStream.Current);

        Assert.Equal(3, factory.SessionOpenedCount);
    }
}
