using Google.Protobuf.Collections;
using Xunit;
using Ydb.Issue;

namespace Ydb.Sdk.Tests;

[Trait("Category", "Unit")]
public class RetryTests
{
    private const StatusCode WrongStatusCode = (StatusCode)123456; // there is no status code with this value 

    [Fact]
    public void GetRetryRuleOutOfRange()
    {
        var retrySettings = new RetrySettings();
        foreach (var statusCode in (StatusCode[])Enum.GetValues(typeof(StatusCode)))
        {
            var exception = Record.Exception(() => retrySettings.GetRetryRule(statusCode));
            Assert.Null(exception);
        }

        Assert.DoesNotContain(WrongStatusCode, (StatusCode[])Enum.GetValues(typeof(StatusCode)));

        Assert.Throws<ArgumentOutOfRangeException>(() => { retrySettings.GetRetryRule(WrongStatusCode); });
    }

    [Fact]
    public void ConvertWrongGrpcStatusCode()
    {
        Assert.DoesNotContain(WrongStatusCode, (StatusCode[])Enum.GetValues(typeof(StatusCode)));

        var status = Status.FromProto(
            statusCode: (StatusIds.Types.StatusCode)WrongStatusCode,
            new RepeatedField<IssueMessage>());
        Assert.Equal(StatusCode.Unspecified, status.StatusCode);
    }
}
