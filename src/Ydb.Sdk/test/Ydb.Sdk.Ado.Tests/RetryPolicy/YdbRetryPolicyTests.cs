using Moq;
using Xunit;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Ado.RetryPolicy;

namespace Ydb.Sdk.Ado.Tests.RetryPolicy;

public class YdbRetryPolicyTests
{
    [Theory]
    [InlineData(StatusCode.BadSession)]
    [InlineData(StatusCode.SessionBusy)]
    [InlineData(StatusCode.SessionExpired)]
    public void GetNextDelay_WhenStatusIsBadSessionOrBusySession_ReturnTimeSpanZero(StatusCode statusCode)
    {
        var ydbRetryPolicy = new YdbRetryPolicy(new YdbRetryPolicyConfig { MaxAttempts = 2 });
        var ydbException = new YdbException(statusCode, "Mock message");

        Assert.Equal(TimeSpan.Zero, ydbRetryPolicy.GetNextDelay(ydbException, 1));
        Assert.Null(ydbRetryPolicy.GetNextDelay(ydbException, 2));
    }

    [Theory]
    [InlineData(StatusCode.ClientTransportUnavailable)]
    [InlineData(StatusCode.Undetermined)]
    [InlineData(StatusCode.ClientTransportCancelled)]
    public void GetNextDelay_WhenStatusIsIdempotenceAndDisableIdempotence_ReturnNull(StatusCode statusCode)
    {
        var ydbRetryPolicy = new YdbRetryPolicy(new YdbRetryPolicyConfig { MaxAttempts = 2 });
        var ydbException = new YdbException(statusCode, "Mock message");

        Assert.Null(ydbRetryPolicy.GetNextDelay(ydbException, 1));
        Assert.Null(ydbRetryPolicy.GetNextDelay(ydbException, 2));
    }

    [Theory]
    [InlineData(StatusCode.Aborted, false, 5, 100, new[] { 5, 10, 20, 40, 80, 100, 100, 100, 100 })]
    [InlineData(StatusCode.Undetermined, true, 5, 100, new[] { 5, 10, 20, 40, 80, 100, 100, 100, 100 })]
    [InlineData(StatusCode.Undetermined, true, 50, 500, new[] { 50, 100, 200, 400, 500, 500, 500, 500, 500 })]
    [InlineData(StatusCode.Undetermined, true, 500, 5000,
        new[] { 500, 1000, 2000, 4000, 5000, 5000, 5000, 5000, 5000 })]
    [InlineData(StatusCode.Undetermined, true, 10000, 30000,
        new[] { 10000, 20000, 30000, 30000, 30000, 30000, 30000, 30000, 30000 })]
    public void GetNextDelay_WhenFullJitterWithFastBackoff_ReturnCalculatedBackoff(StatusCode statusCode,
        bool enableRetryIdempotence, int baseDelayMs, int capMs, int[] delayCeilings)
    {
        var mockRandom = new Mock<IRandom>(MockBehavior.Strict);
        var ydbRetryPolicy = new YdbRetryPolicy(new YdbRetryPolicyConfig
        {
            EnableRetryIdempotence = enableRetryIdempotence,
            FastBackoffBaseMs = baseDelayMs,
            FastCapBackoffMs = capMs
        }, mockRandom.Object);
        var ydbException = new YdbException(statusCode, "Mock message");

        for (var index = 0; index < delayCeilings.Length; index++)
        {
            var ceiling = delayCeilings[index];
            mockRandom.Setup(random => random.Next(ceiling + 1)).Returns(ceiling);
            Assert.Equal(TimeSpan.FromMilliseconds(ceiling), ydbRetryPolicy.GetNextDelay(ydbException, index + 1));
        }

        Assert.Null(ydbRetryPolicy.GetNextDelay(ydbException, 10));
    }

    [Theory]
    [InlineData(StatusCode.Unavailable, false)]
    [InlineData(StatusCode.ClientTransportUnknown, true)]
    [InlineData(StatusCode.ClientTransportUnavailable, true)]
    [InlineData(StatusCode.ClientTransportCancelled, true)]
    public void GetNextDelay_WhenEqualJitterWithFastBackoff_ReturnCalculatedBackoff(StatusCode statusCode,
        bool enableRetryIdempotence)
    {
        var mockRandom = new Mock<IRandom>(MockBehavior.Strict);
        var ydbRetryPolicy = new YdbRetryPolicy(new YdbRetryPolicyConfig
        {
            EnableRetryIdempotence = enableRetryIdempotence,
            FastBackoffBaseMs = 5,
            FastCapBackoffMs = 50
        }, mockRandom.Object);
        var ydbException = new YdbException(statusCode, "Mock message");

        mockRandom.Setup(random => random.Next(3)).Returns(1);
        Assert.Equal(TimeSpan.FromMilliseconds(4), ydbRetryPolicy.GetNextDelay(ydbException, 1));

        mockRandom.Setup(random => random.Next(6)).Returns(5);
        Assert.Equal(TimeSpan.FromMilliseconds(10), ydbRetryPolicy.GetNextDelay(ydbException, 2));

        mockRandom.Setup(random => random.Next(11)).Returns(8);
        Assert.Equal(TimeSpan.FromMilliseconds(18), ydbRetryPolicy.GetNextDelay(ydbException, 3));

        mockRandom.Setup(random => random.Next(21)).Returns(15);
        Assert.Equal(TimeSpan.FromMilliseconds(35), ydbRetryPolicy.GetNextDelay(ydbException, 4));

        mockRandom.Setup(random => random.Next(26)).Returns(11);
        Assert.Equal(TimeSpan.FromMilliseconds(36), ydbRetryPolicy.GetNextDelay(ydbException, 5));
    }

    [Theory]
    [InlineData(StatusCode.Overloaded, false)]
    [InlineData(StatusCode.ClientTransportResourceExhausted, false)]
    public void GetNextDelay_WhenEqualJitterWithSlowBackoff_ReturnCalculatedBackoff(StatusCode statusCode,
        bool enableRetryIdempotence)
    {
        var mockRandom = new Mock<IRandom>(MockBehavior.Strict);
        var ydbRetryPolicy = new YdbRetryPolicy(new YdbRetryPolicyConfig
        {
            EnableRetryIdempotence = enableRetryIdempotence,
            SlowBackoffBaseMs = 100,
            SlowCapBackoffMs = 1000
        }, mockRandom.Object);
        var ydbException = new YdbException(statusCode, "Mock message");

        mockRandom.Setup(random => random.Next(51)).Returns(27);
        Assert.Equal(TimeSpan.FromMilliseconds(77), ydbRetryPolicy.GetNextDelay(ydbException, 1));

        mockRandom.Setup(random => random.Next(101)).Returns(5);
        Assert.Equal(TimeSpan.FromMilliseconds(105), ydbRetryPolicy.GetNextDelay(ydbException, 2));

        mockRandom.Setup(random => random.Next(201)).Returns(123);
        Assert.Equal(TimeSpan.FromMilliseconds(323), ydbRetryPolicy.GetNextDelay(ydbException, 3));

        mockRandom.Setup(random => random.Next(401)).Returns(301);
        Assert.Equal(TimeSpan.FromMilliseconds(701), ydbRetryPolicy.GetNextDelay(ydbException, 4));

        mockRandom.Setup(random => random.Next(501)).Returns(257);
        Assert.Equal(TimeSpan.FromMilliseconds(757), ydbRetryPolicy.GetNextDelay(ydbException, 5));
    }

    [Theory]
    [InlineData(StatusCode.SchemeError)]
    [InlineData(StatusCode.Unspecified)]
    [InlineData(StatusCode.BadRequest)]
    [InlineData(StatusCode.Unauthorized)]
    [InlineData(StatusCode.InternalError)]
    [InlineData(StatusCode.GenericError)]
    [InlineData(StatusCode.Timeout)]
    [InlineData(StatusCode.PreconditionFailed)]
    [InlineData(StatusCode.AlreadyExists)]
    [InlineData(StatusCode.NotFound)]
    [InlineData(StatusCode.Cancelled)]
    [InlineData(StatusCode.Unsupported)]
    [InlineData(StatusCode.Success)]
    [InlineData(StatusCode.ClientTransportTimeout)]
    [InlineData(StatusCode.ClientTransportUnimplemented)]
    [InlineData(StatusCode.StreamCancelled)]
    public void GetNextDelay_WhenStatusCodeIsNotRetriable_ReturnNull(StatusCode statusCode) =>
        Assert.Null(YdbRetryPolicy.IdempotenceDefault.GetNextDelay(new YdbException(statusCode, "Mock message"), 1));
}
