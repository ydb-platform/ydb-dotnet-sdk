#if !EFCORE9
using Microsoft.EntityFrameworkCore.Query.Translations.Temporal;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query.Translations.Temporal;

public class TimeOnlyTranslationsYdbTest(BasicTypesQueryYdbFixture fixture)
    : TimeOnlyTranslationsTestBase<BasicTypesQueryYdbFixture>(fixture)
{
    [ConditionalFact(Skip = "TimeOnly.Add(TimeSpan) translation is not supported.")]
    public override Task Add_TimeSpan() => base.Add_TimeSpan();

    [ConditionalFact(Skip = "TimeOnly.FromDateTime translation is not supported.")]
    public override Task FromDateTime_compared_to_constant() => base.FromDateTime_compared_to_constant();

    [ConditionalFact(Skip = "TimeOnly.FromDateTime translation is not supported.")]
    public override Task FromDateTime_compared_to_parameter() => base.FromDateTime_compared_to_parameter();

    [ConditionalFact(Skip = "TimeOnly.FromDateTime translation is not supported.")]
    public override Task FromDateTime_compared_to_property() => base.FromDateTime_compared_to_property();

    [ConditionalFact(Skip = "TimeOnly.FromTimeSpan translation is not supported.")]
    public override Task FromTimeSpan_compared_to_parameter() => base.FromTimeSpan_compared_to_parameter();

    [ConditionalFact(Skip = "TimeOnly.FromTimeSpan translation is not supported.")]
    public override Task FromTimeSpan_compared_to_property() => base.FromTimeSpan_compared_to_property();

    [ConditionalFact(Skip = "TimeOnly component translation is not supported.")]
    public override Task Hour() => base.Hour();

    [ConditionalFact(Skip = "TimeOnly.IsBetween translation is not supported.")]
    public override Task IsBetween() => base.IsBetween();

    [ConditionalFact(Skip = "TimeOnly component translation is not supported.")]
    public override Task Microsecond() => base.Microsecond();

    [ConditionalFact(Skip = "TimeOnly component translation is not supported.")]
    public override Task Millisecond() => base.Millisecond();

    [ConditionalFact(Skip = "TimeOnly component translation is not supported.")]
    public override Task Minute() => base.Minute();

    [ConditionalFact(Skip = "TimeOnly component translation is not supported.")]
    public override Task Nanosecond() => base.Nanosecond();

    [ConditionalFact(Skip = "TimeOnly.FromTimeSpan translation is not supported.")]
    public override Task Order_by_FromTimeSpan() => base.Order_by_FromTimeSpan();

    [ConditionalFact(Skip = "TimeOnly component translation is not supported.")]
    public override Task Second() => base.Second();

    [ConditionalFact(Skip = "TimeOnly subtraction is not mapped to TimeSpan.")]
    public override Task Subtract() => base.Subtract();
}
#endif
