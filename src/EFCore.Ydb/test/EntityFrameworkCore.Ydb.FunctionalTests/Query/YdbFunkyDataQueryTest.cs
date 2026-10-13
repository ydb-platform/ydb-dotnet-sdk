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
    public Task String_equality_with_backslash_constant(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<FunkyCustomer>().Where(c => c.FirstName == @"\%Bar"),
            assertEmpty: true);

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public Task String_equality_with_quote_constant(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<FunkyCustomer>().Where(c => c.FirstName == "O'Brien"),
            assertEmpty: true);

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public Task String_equality_with_null_character_constant(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<FunkyCustomer>().Where(c => c.FirstName == "\012"),
            assertEmpty: true);

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public async Task String_methods_with_escape_character_constant(bool async)
    {
        await AssertQuery(
            async,
            ss => ss.Set<FunkyCustomer>().Where(c => c.FirstName != null
                                                     && ("!" + c.FirstName + "!").Contains("!")
                                                     && ("!" + c.FirstName + "!").StartsWith("!")
                                                     && ("!" + c.FirstName + "!").EndsWith("!")),
            ss => ss.Set<FunkyCustomer>().Where(c => c.FirstName != null));

        await AssertQuery(
            async,
            ss => ss.Set<FunkyCustomer>().Where(c => c.FirstName.Contains("\\")),
            ss => ss.Set<FunkyCustomer>().Where(c => c.FirstName != null && c.FirstName.Contains("\\")),
            assertEmpty: true);
    }

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

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public Task String_FirstOrDefault_and_LastOrDefault_use_UTF16_code_units(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<FunkyCustomer>().OrderBy(e => e.Id).Select(e => new
            {
                first = (char?)("\U0001F642" + e.FirstName + "\U0001F642").FirstOrDefault(),
                last = (char?)("\U0001F642" + e.FirstName + "\U0001F642").LastOrDefault()
            }),
            assertOrder: true,
            elementAsserter: (e, a) =>
            {
                AssertEqual(e.first, a.first);
                AssertEqual(e.last, a.last);
            });

    public class YdbFunkyDataQueryFixture : FunkyDataQueryFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;
    }
}
