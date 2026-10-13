using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.FunkyDataModel;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class YdbFunkyDataQueryTest(YdbFunkyDataQueryTest.YdbFunkyDataQueryFixture fixture)
    : FunkyDataQueryTestBase<YdbFunkyDataQueryTest.YdbFunkyDataQueryFixture>(fixture)
{
    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public Task String_methods_with_escape_character_parameter(bool async)
    {
        var pattern = "!";

        return AssertQuery(
            async,
            ss => ss.Set<FunkyCustomer>().Where(c => c.FirstName != null
                                                     && ("!" + c.FirstName + "!").Contains(pattern)
                                                     && ("!" + c.FirstName + "!").StartsWith(pattern)
                                                     && ("!" + c.FirstName + "!").EndsWith(pattern)),
            ss => ss.Set<FunkyCustomer>().Where(c => c.FirstName != null));
    }

    [ConditionalTheory(Skip = "TODO: Fix LIKE wildcard escaping for constant string patterns")]
    [MemberData(nameof(IsAsyncData))]
    public override Task String_contains_on_argument_with_wildcard_constant(bool async)
        => base.String_contains_on_argument_with_wildcard_constant(async);

    [ConditionalTheory(Skip = "TODO: Fix LIKE wildcard escaping for constant string patterns")]
    [MemberData(nameof(IsAsyncData))]
    public override Task String_starts_with_on_argument_with_wildcard_constant(bool async)
        => base.String_starts_with_on_argument_with_wildcard_constant(async);

    [ConditionalTheory(Skip = "TODO: Fix LIKE wildcard escaping for constant string patterns")]
    [MemberData(nameof(IsAsyncData))]
    public override Task String_ends_with_on_argument_with_wildcard_constant(bool async)
        => base.String_ends_with_on_argument_with_wildcard_constant(async);

    [ConditionalTheory(Skip = "TODO: Translate FirstOrDefault/LastOrDefault for nullable strings")]
    [MemberData(nameof(IsAsyncData))]
    public override Task String_FirstOrDefault_and_LastOrDefault(bool async)
        => base.String_FirstOrDefault_and_LastOrDefault(async);

    public class YdbFunkyDataQueryFixture : FunkyDataQueryFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;
    }
}
