#if !EFCORE9
using Microsoft.EntityFrameworkCore.Query.Translations.Temporal;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query.Translations.Temporal;

public class DateTimeTimeOfDayTranslationsYdbTest(BasicTypesQueryYdbFixture fixture)
    : IClassFixture<BasicTypesQueryYdbFixture>
{
    [Fact]
    public Task TimeOfDay() => new MicrosoftTests(fixture).TimeOfDay();

    private sealed class MicrosoftTests(BasicTypesQueryYdbFixture fixture)
        : DateTimeTranslationsTestBase<BasicTypesQueryYdbFixture>(fixture);
}
#endif
