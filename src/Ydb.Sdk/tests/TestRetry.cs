using Xunit;
using Xunit.Abstractions;

namespace Ydb.Sdk.Tests;

[Trait("Category", "Unit")]
public class TestRetry
{
    // ReSharper disable once NotAccessedField.Local
    private readonly ITestOutputHelper _output;

    public TestRetry(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void GetRetryRuleOutOfRange()
    {
        var retrySettings = new RetrySettings();
        foreach (var statusCode in (StatusCode[]) Enum.GetValues(typeof(StatusCode)))
        {
            var exception = Record.Exception(() => retrySettings.GetRetryRule(statusCode));
            Assert.Null(exception);
        }
    }
}