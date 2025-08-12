using Xunit;
using Ydb.Sdk.Ado.Retry;

namespace Ydb.Sdk.Ado.Tests;

public class DefaultRetryPolicyTests : TestBase
{
    [Fact]
    public void GetDelay_WhenAttemptOne_UsesDefaultDelayDelegate()
    {
        var config = new RetryConfig
        {
            DefaultDelay = (ex, attempt) =>
            {
                _ = ex;
                _ = attempt;
                return TimeSpan.FromMilliseconds(100);
            },
            MaxDelay = TimeSpan.FromMilliseconds(500)
        };

        var policy = new DefaultRetryPolicy(config);

        var delay = policy.GetDelay(new Exception("test"), 1);
        Assert.Equal(TimeSpan.FromMilliseconds(100), delay);
    }

    [Fact]
    public void GetDelay_WhenAttemptOne_ReturnsBaseDelay_NoJitter()
    {
        var config = new RetryConfig
        {
            BaseDelay = TimeSpan.FromMilliseconds(100),
            Exponent = 2.0,
            JitterFraction = 0.0,
            MaxDelay = TimeSpan.FromMilliseconds(500),
            DefaultDelay = (ex, attempt) =>
            {
                _ = ex;
                attempt = Math.Max(1, attempt);
                var baseMs = 100.0 * Math.Pow(2.0, attempt - 1);
                var ms = Math.Min(baseMs, 500.0);
                return TimeSpan.FromMilliseconds(ms);
            }
        };

        var policy = new DefaultRetryPolicy(config);

        var delay = policy.GetDelay(new Exception("test"), 1);
        Assert.Equal(TimeSpan.FromMilliseconds(100), delay);
    }

    [Fact]
    public void GetDelay_WhenStatusHasOverride_ReturnsPerStatusDelay()
    {
        var config = new RetryConfig();
        config.PerStatusDelay[StatusCode.Unavailable] = attempt =>
        {
            _ = attempt;
            return TimeSpan.FromMilliseconds(123);
        };
        var policy = new DefaultRetryPolicy(config);

        var ex = new YdbException(StatusCode.Unavailable, "unavailable");
        var delay = policy.GetDelay(ex, 2);

        Assert.Equal(TimeSpan.FromMilliseconds(123), delay);
    }

    [Fact]
    public void CanRetry_WhenTransientYdbException_ReturnsTrue()
    {
        var config = new RetryConfig();
        var policy = new DefaultRetryPolicy(config);
        var ex = new YdbException(StatusCode.Aborted, "transient");

        Assert.True(policy.CanRetry(ex, isIdempotent: false));
    }

    [Fact]
    public void CanRetry_WhenOverloaded_RetriesRegardlessOfIdempotency()
    {
        var policy = new DefaultRetryPolicy(new RetryConfig());
        var ex = new YdbException(StatusCode.Overloaded, "overloaded");

        Assert.True(policy.CanRetry(ex, isIdempotent: false));
        Assert.True(policy.CanRetry(ex, isIdempotent: true));
    }

    [Fact]
    public void CanRetry_WhenTimeoutException_ReturnsFalse()
    {
        var config = new RetryConfig();
        var policy = new DefaultRetryPolicy(config);

        Assert.False(policy.CanRetry(new TimeoutException(), isIdempotent: true));
    }

    [Fact]
    public void CanRetry_WhenUserCancelled_ReturnsFalse()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var config = new RetryConfig();
        var policy = new DefaultRetryPolicy(config);

        var ex = new OperationCanceledException(cts.Token);
        Assert.False(policy.CanRetry(ex, isIdempotent: true));
    }

    [Fact]
    public void GetDelay_WhenDelayExceedsMaxDelay_IsCappedToMaxDelay()
    {
        var config = new RetryConfig
        {
            MaxDelay = TimeSpan.FromMilliseconds(500),
            DefaultDelay = (_, _) => TimeSpan.FromMilliseconds(1000)
        };
        var policy = new DefaultRetryPolicy(config);

        var delay = policy.GetDelay(new Exception("test"), 1);

        Assert.Equal(TimeSpan.FromMilliseconds(500), delay);
    }

    [Fact]
    public void CanRetry_WhenOperationCanceledWithoutToken_ReturnsFalse()
    {
        var config = new RetryConfig();
        var policy = new DefaultRetryPolicy(config);

        var ex = new OperationCanceledException();
        Assert.False(policy.CanRetry(ex, isIdempotent: true));
    }
}
