using System.Data;
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
            DefaultDelay = (_, __) => TimeSpan.FromMilliseconds(100),
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
        config.PerStatusDelay[StatusCode.Unavailable] = attempt => TimeSpan.FromMilliseconds(123);
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
    public void CanRetry_WhenTimeoutException_ReturnsTrue()
    {
        var config = new RetryConfig();
        var policy = new DefaultRetryPolicy(config);

        Assert.True(policy.CanRetry(new TimeoutException(), isIdempotent: true));
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
    public void IsStreaming_WhenSequentialAccess_ReturnsTrue()
    {
        var config = new RetryConfig();
        var policy = new DefaultRetryPolicy(config);
        var cmd = new DummyCommand();

        Assert.True(policy.IsStreaming(cmd, CommandBehavior.SequentialAccess));
    }

    [Fact]
    public void IsStreaming_WhenCustomConfigDelegateUsed_ReturnsTrue()
    {
        var config = new RetryConfig { IsStreaming = (c, b) => true };
        var policy = new DefaultRetryPolicy(config);
        var cmd = new DummyCommand();

        Assert.True(policy.IsStreaming(cmd, CommandBehavior.Default));
    }
    
    [Fact]
    public void GetDelay_WhenDelayExceedsMaxDelay_IsCappedToMaxDelay()
    {
        var config = new RetryConfig
        {
            MaxDelay = TimeSpan.FromMilliseconds(500),
            DefaultDelay = (_, __) => TimeSpan.FromMilliseconds(1000)
        };
        var policy = new DefaultRetryPolicy(config);

        var delay = policy.GetDelay(new Exception("test"), 1);

        Assert.Equal(TimeSpan.FromMilliseconds(500), delay);
    }

    [Fact]
    public void CanRetry_WhenOperationCanceledWithoutToken_ReturnsTrue()
    {
        var config = new RetryConfig();
        var policy = new DefaultRetryPolicy(config);

        var ex = new OperationCanceledException();
        Assert.True(policy.CanRetry(ex, isIdempotent: true));
    }

    private class DummyCommand : System.Data.Common.DbCommand
    {
        public override string CommandText { get; set; } = string.Empty;
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; } = CommandType.Text;
        protected override System.Data.Common.DbConnection DbConnection { get; set; }
        protected override System.Data.Common.DbParameterCollection DbParameterCollection { get; } = null!;
        protected override System.Data.Common.DbTransaction DbTransaction { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        public override void Cancel() { }
        public override int ExecuteNonQuery() => 0;
        public override object ExecuteScalar() => null!;
        public override void Prepare() { }
        protected override System.Data.Common.DbParameter CreateDbParameter() => throw new NotImplementedException();
        protected override System.Data.Common.DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotImplementedException();
    }
}
