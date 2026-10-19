#if !EFCORE9
using Microsoft.EntityFrameworkCore.Query.Translations.Temporal;
using Microsoft.EntityFrameworkCore.TestUtilities.Xunit;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query.Translations.Temporal;

[UseCulture("en-US")]
public class DateTimeTranslationsYdbTest(BasicTypesQueryYdbFixture fixture)
    : DateTimeTranslationsTestBase<BasicTypesQueryYdbFixture>(fixture)
{
    [ConditionalFact(Skip = "DateTime.Now translation is not supported.")]
    public override Task Now() => base.Now();

    [ConditionalFact(Skip = "Timestamp64 does not preserve the original fixture's sub-microsecond DateTime precision.")]
    public override Task UtcNow() => base.UtcNow();

    [ConditionalFact(Skip = "DateTime.Today translation is not supported.")]
    public override Task Today() => base.Today();

    [ConditionalFact(Skip = "DateTime subtraction with TimeSpan.TotalDays is not supported.")]
    public override Task subtract_and_TotalDays() => base.subtract_and_TotalDays();
}
#endif
