#if !EFCORE9
using Microsoft.EntityFrameworkCore.Query.Translations.Temporal;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query.Translations.Temporal;

public class DateOnlyTranslationsYdbTest(BasicTypesQueryYdbFixture fixture)
    : DateOnlyTranslationsTestBase<BasicTypesQueryYdbFixture>(fixture)
{
    [ConditionalFact(Skip = "DateOnly.DayNumber translation is not supported.")]
    public override Task DayNumber() => base.DayNumber();

    [ConditionalFact(Skip = "DateOnly.DayNumber translation is not supported.")]
    public override Task DayNumber_subtraction() => base.DayNumber_subtraction();

    [ConditionalFact(Skip = "Timestamp64 does not preserve the original fixture's sub-microsecond DateTime precision.")]
    public override Task FromDateTime_compared_to_property() => base.FromDateTime_compared_to_property();

    [ConditionalFact(Skip = "DateOnly.ToDateTime translation is not supported.")]
    public override Task ToDateTime_property_with_constant_TimeOnly()
        => base.ToDateTime_property_with_constant_TimeOnly();

    [ConditionalFact(Skip = "DateOnly.ToDateTime translation is not supported.")]
    public override Task ToDateTime_property_with_property_TimeOnly()
        => base.ToDateTime_property_with_property_TimeOnly();

    [ConditionalFact(Skip = "DateOnly.ToDateTime translation is not supported.")]
    public override Task ToDateTime_constant_DateTime_with_property_TimeOnly()
        => base.ToDateTime_constant_DateTime_with_property_TimeOnly();

    [ConditionalFact(Skip = "DateOnly.ToDateTime translation is not supported.")]
    public override Task ToDateTime_with_complex_DateTime() => base.ToDateTime_with_complex_DateTime();

    [ConditionalFact(Skip = "DateOnly.ToDateTime and TimeOnly.AddHours translations are not supported.")]
    public override Task ToDateTime_with_complex_TimeOnly() => base.ToDateTime_with_complex_TimeOnly();
}
#endif
