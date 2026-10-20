#if !EFCORE9
using Microsoft.EntityFrameworkCore.Query.Translations.Temporal;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query.Translations.Temporal;

public class DateOnlyTranslationsYdbTest(BasicTypesQueryYdbFixture fixture)
    : DateOnlyTranslationsTestBase<BasicTypesQueryYdbFixture>(fixture)
{
    [ConditionalFact(Skip = "Timestamp64 does not preserve the original fixture's sub-microsecond DateTime precision.")]
    public override Task FromDateTime_compared_to_property() => base.FromDateTime_compared_to_property();
}
#endif
