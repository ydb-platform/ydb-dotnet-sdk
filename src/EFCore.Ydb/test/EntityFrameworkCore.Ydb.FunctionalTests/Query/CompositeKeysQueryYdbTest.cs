using Microsoft.EntityFrameworkCore.Query;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

// TODO: Unable to translate expression
public class CompositeKeysQueryYdbTest(CompositeKeysQueryYdbFixture fixture)
    : CompositeKeysQueryRelationalTestBase<CompositeKeysQueryYdbFixture>(fixture)
{
    [ConditionalTheory(Skip = "TODO: Unable to translate")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Projecting_multiple_collections_on_multiple_levels_some_explicit_ordering(bool async) =>
        base.Projecting_multiple_collections_on_multiple_levels_some_explicit_ordering(async);
}
