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
    public void GetNextDelay_WhenStatusIsBadSessionOrBusySession_ReturnTimeSpanZero(StatusCode statusCode)
    {
        var ydbRetryPolicy = new YdbRetryPolicy(new YdbRetryPolicyConfig { MaxAttempts = 2 });
        var ydbException = new YdbException(statusCode, "Mock message");

        Assert.Equal(TimeSpan.Zero, ydbRetryPolicy.GetNextDelay(ydbException, 0));
        Assert.Null(ydbRetryPolicy.GetNextDelay(ydbException, 1));
    }

    [Theory]
    [InlineData(StatusCode.ClientTransportUnavailable)]
    [InlineData(StatusCode.Undetermined)]
    public void GetNextDelay_WhenStatusIsIdempotenceAndDisableIdempotence_ReturnNull(StatusCode statusCode)
    {
        var ydbRetryPolicy = new YdbRetryPolicy(new YdbRetryPolicyConfig { MaxAttempts = 2 });
        var ydbException = new YdbException(statusCode, "Mock message");

        Assert.Null(ydbRetryPolicy.GetNextDelay(ydbException, 0));
        Assert.Null(ydbRetryPolicy.GetNextDelay(ydbException, 1));
    }

    [Theory]
    [InlineData(StatusCode.Aborted, false)]
    [InlineData(StatusCode.Undetermined, true)]
    public void GetNextDelay_WhenFullJitterWithFastBackoff_ReturnCalculatedBackoff(StatusCode statusCode,
        bool enableRetryIdempotence)
    {
        var mockRandom = new Mock<IRandom>(MockBehavior.Strict);
        var ydbRetryPolicy = new YdbRetryPolicy(new YdbRetryPolicyConfig
        {
            EnableRetryIdempotence = enableRetryIdempotence,
            FastBackoffBaseMs = 5,
            FastCapBackoffMs = 100
        }, mockRandom.Object);
        var ydbException = new YdbException(statusCode, "Mock message");

        mockRandom.Setup(random => random.Next(6)).Returns(2);
        Assert.Equal(TimeSpan.FromMilliseconds(2), ydbRetryPolicy.GetNextDelay(ydbException, 0));

        mockRandom.Setup(random => random.Next(11)).Returns(7);
        Assert.Equal(TimeSpan.FromMilliseconds(7), ydbRetryPolicy.GetNextDelay(ydbException, 1));

        mockRandom.Setup(random => random.Next(21)).Returns(14);
        Assert.Equal(TimeSpan.FromMilliseconds(14), ydbRetryPolicy.GetNextDelay(ydbException, 2));

        mockRandom.Setup(random => random.Next(41)).Returns(23);
        Assert.Equal(TimeSpan.FromMilliseconds(23), ydbRetryPolicy.GetNextDelay(ydbException, 3));

        mockRandom.Setup(random => random.Next(81)).Returns(53);
        Assert.Equal(TimeSpan.FromMilliseconds(53), ydbRetryPolicy.GetNextDelay(ydbException, 4));

        mockRandom.Setup(random => random.Next(101)).Returns(89);
        Assert.Equal(TimeSpan.FromMilliseconds(89), ydbRetryPolicy.GetNextDelay(ydbException, 5));
    }

    [Theory]
    [InlineData(StatusCode.Unavailable, false)]
    [InlineData(StatusCode.ClientTransportUnknown, true)]
    [InlineData(StatusCode.ClientTransportUnavailable, true)]
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
        Assert.Equal(TimeSpan.FromMilliseconds(4), ydbRetryPolicy.GetNextDelay(ydbException, 0));

        mockRandom.Setup(random => random.Next(6)).Returns(5);
        Assert.Equal(TimeSpan.FromMilliseconds(10), ydbRetryPolicy.GetNextDelay(ydbException, 1));

        mockRandom.Setup(random => random.Next(11)).Returns(8);
        Assert.Equal(TimeSpan.FromMilliseconds(18), ydbRetryPolicy.GetNextDelay(ydbException, 2));

        mockRandom.Setup(random => random.Next(21)).Returns(15);
        Assert.Equal(TimeSpan.FromMilliseconds(35), ydbRetryPolicy.GetNextDelay(ydbException, 3));

        mockRandom.Setup(random => random.Next(26)).Returns(11);
        Assert.Equal(TimeSpan.FromMilliseconds(36), ydbRetryPolicy.GetNextDelay(ydbException, 4));
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
        Assert.Equal(TimeSpan.FromMilliseconds(77), ydbRetryPolicy.GetNextDelay(ydbException, 0));

        mockRandom.Setup(random => random.Next(101)).Returns(5);
        Assert.Equal(TimeSpan.FromMilliseconds(105), ydbRetryPolicy.GetNextDelay(ydbException, 1));

        mockRandom.Setup(random => random.Next(201)).Returns(123);
        Assert.Equal(TimeSpan.FromMilliseconds(323), ydbRetryPolicy.GetNextDelay(ydbException, 2));

        mockRandom.Setup(random => random.Next(401)).Returns(301);
        Assert.Equal(TimeSpan.FromMilliseconds(701), ydbRetryPolicy.GetNextDelay(ydbException, 3));

        mockRandom.Setup(random => random.Next(501)).Returns(257);
        Assert.Equal(TimeSpan.FromMilliseconds(757), ydbRetryPolicy.GetNextDelay(ydbException, 4));
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
    public void GetNextDelay_WhenStatusCodeIsNotRetriable_ReturnNull(StatusCode statusCode) =>
        Assert.Null(new YdbRetryPolicy(new YdbRetryPolicyConfig { EnableRetryIdempotence = true })
            .GetNextDelay(new YdbException(statusCode, "Mock message"), 0));
}
