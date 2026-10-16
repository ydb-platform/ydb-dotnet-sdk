using Grpc.Core;
using Xunit;
using Ydb.Query;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Ado.RetryPolicy;
using Ydb.Sdk.Ado.Session;
using Ydb.Sdk.Ado.Tests.Utils;

namespace Ydb.Sdk.Ado.Tests.Session;

public class RetryableSessionTests
{
    [Theory]
    [InlineData(Grpc.Core.StatusCode.Cancelled, true, StatusCode.ClientCancelled, true)]
    [InlineData(Grpc.Core.StatusCode.Cancelled, false, StatusCode.ClientCancelled, false)]
    [InlineData(Grpc.Core.StatusCode.DeadlineExceeded, true, StatusCode.ClientTransportTimeout, false)]
    [InlineData(Grpc.Core.StatusCode.Unavailable, true, StatusCode.ClientTransportUnavailable, true)]
    public async Task MoveNextAsync_WhenTransportFails_RetriesOnlyIdempotentCancellation(
        Grpc.Core.StatusCode grpcStatus,
        bool enableRetryIdempotence,
        StatusCode expectedStatus,
        bool shouldRetry)
    {
        var attempts = 0;
        var factory = new MockPoolingSessionFactory(1)
        {
            IsBroken = _ => false,
            ExecuteQuery = _ => ++attempts == 1
                ? throw new YdbException(new RpcException(new Status(grpcStatus, "Transport failure")))
                : new MockAsyncEnumerator<ExecuteQueryResponsePart>(
                    new List<ExecuteQueryResponsePart> { new() { Status = StatusIds.Types.StatusCode.Success } })
        };
        var retryableSession = new RetryableSession(new PoolingSessionSource<MockPoolingSession>(
                factory, new YdbConnectionStringBuilder { MaxPoolSize = 1 }),
            new YdbRetryPolicyExecutor(new YdbRetryPolicy(new YdbRetryPolicyConfig
            {
                EnableRetryIdempotence = enableRetryIdempotence,
                FastBackoffBaseMs = 0,
                MaxAttempts = 2
            })));
        var stream = await retryableSession.ExecuteQuery(
            "SELECT 1", new Dictionary<string, TypedValue>(), new GrpcRequestSettings(), null);

        if (shouldRetry)
        {
            Assert.True(await stream.MoveNextAsync());
            Assert.Equal(StatusIds.Types.StatusCode.Success, stream.Current.Status);
        }
        else
        {
            Assert.Equal(expectedStatus,
                (await Assert.ThrowsAsync<YdbException>(async () => await stream.MoveNextAsync())).Code);
        }

        Assert.Equal(shouldRetry ? 2 : 1, attempts);
        Assert.Equal(shouldRetry ? 2 : 1, factory.SessionOpenedCount);
    }

    [Fact]
    public async Task MoveNextAsync_WhenCallerCancelsDuringRpc_DoesNotRetry()
    {
        using var cancellation = new CancellationTokenSource();
        var attempts = 0;
        var factory = new MockPoolingSessionFactory(1)
        {
            IsBroken = _ => false,
            ExecuteQuery = _ =>
            {
                attempts++;
                cancellation.Cancel();
                throw new YdbException(new RpcException(new Status(Grpc.Core.StatusCode.Cancelled, "RPC cancelled")));
            }
        };
        var retryableSession = new RetryableSession(new PoolingSessionSource<MockPoolingSession>(
                factory, new YdbConnectionStringBuilder { MaxPoolSize = 1 }),
            new YdbRetryPolicyExecutor(new YdbRetryPolicy(new YdbRetryPolicyConfig
            {
                EnableRetryIdempotence = true,
                FastBackoffBaseMs = 0,
                MaxAttempts = 2
            })));
        var stream = await retryableSession.ExecuteQuery(
            "SELECT 1", new Dictionary<string, TypedValue>(), new GrpcRequestSettings(), null);

        var exception =
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await stream.MoveNextAsync(cancellation.Token));

        Assert.Equal(cancellation.Token, exception.CancellationToken);
        Assert.Equal(1, attempts);
        Assert.Equal(1, factory.SessionOpenedCount);
    }

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
