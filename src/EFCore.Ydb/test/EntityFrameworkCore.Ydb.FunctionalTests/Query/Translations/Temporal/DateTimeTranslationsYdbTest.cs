#if !EFCORE9
using Microsoft.EntityFrameworkCore.Query.Translations.Temporal;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query.Translations.Temporal;

public class DateTimeTranslationsYdbTest(BasicTypesQueryYdbFixture fixture)
    : DateTimeTranslationsTestBase<BasicTypesQueryYdbFixture>(fixture)
{
    [ConditionalFact(Skip = "DateTime.Now translation is not supported.")]
    public override Task Now() => base.Now();

    [ConditionalFact(Skip = "Timestamp64 does not preserve the original fixture's sub-microsecond DateTime precision.")]
    public override Task UtcNow() => base.UtcNow();

    [ConditionalFact(Skip = "DateTime.Today translation is not supported.")]
    public override Task Today() => base.Today();

    [ConditionalFact]
    public override Task Date() => base.Date();

    [ConditionalFact(Skip = "DateTime.AddYears translation uses TM instead of TM64 for Timestamp64.")]
    public override Task AddYear() => base.AddYear();

    [ConditionalFact]
    public override Task Year() => base.Year();

    [ConditionalFact]
    public override Task Month() => base.Month();

    [ConditionalFact]
    public override Task DayOfYear() => base.DayOfYear();

    [ConditionalFact]
    public override Task Day() => base.Day();

    [ConditionalFact]
    public override Task Hour() => base.Hour();

    [ConditionalFact]
    public override Task Minute() => base.Minute();

    [ConditionalFact]
    public override Task Second() => base.Second();

    [ConditionalFact]
    public override Task Millisecond() => base.Millisecond();

    [ConditionalFact]
    public override Task TimeOfDay() => base.TimeOfDay();

    [ConditionalFact(Skip = "DateTime subtraction with TimeSpan.TotalDays is not supported.")]
    public override Task subtract_and_TotalDays() => base.subtract_and_TotalDays();

    [ConditionalFact]
    public override Task Parse_with_constant() => base.Parse_with_constant();

    [ConditionalFact]
    public override Task Parse_with_parameter() => base.Parse_with_parameter();

    [ConditionalFact]
    public override Task New_with_constant() => base.New_with_constant();

    [ConditionalFact]
    public override Task New_with_parameters() => base.New_with_parameters();
}
#endif
