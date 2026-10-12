using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class StringColumnPatternYdbTest(StringColumnPatternYdbTest.StringColumnPatternYdbFixture fixture)
    : IClassFixture<StringColumnPatternYdbTest.StringColumnPatternYdbFixture>
{
    private readonly MicrosoftTests _tests = new(fixture);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task String_starts_with_on_argument_with_wildcard_column(bool async)
        => _tests.String_starts_with_on_argument_with_wildcard_column(async);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task String_starts_with_on_argument_with_wildcard_column_negated(bool async)
        => _tests.String_starts_with_on_argument_with_wildcard_column_negated(async);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task String_ends_with_on_argument_with_wildcard_column(bool async)
        => _tests.String_ends_with_on_argument_with_wildcard_column(async);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task String_ends_with_on_argument_with_wildcard_column_negated(bool async)
        => _tests.String_ends_with_on_argument_with_wildcard_column_negated(async);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task String_ends_with_inside_conditional(bool async)
        => _tests.String_ends_with_inside_conditional(async);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task String_ends_with_inside_conditional_negated(bool async)
        => _tests.String_ends_with_inside_conditional_negated(async);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task String_ends_with_equals_nullable_column(bool async)
        => _tests.String_ends_with_equals_nullable_column(async);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task String_ends_with_not_equals_nullable_column(bool async)
        => _tests.String_ends_with_not_equals_nullable_column(async);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task String_contains_on_argument_with_wildcard_column(bool async)
        => _tests.String_contains_on_argument_with_wildcard_column(async);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task String_contains_on_argument_with_wildcard_column_negated(bool async)
        => _tests.String_contains_on_argument_with_wildcard_column_negated(async);

    private class MicrosoftTests(StringColumnPatternYdbFixture fixture)
        : FunkyDataQueryTestBase<StringColumnPatternYdbFixture>(fixture);

    public class StringColumnPatternYdbFixture
        : FunkyDataQueryTestBase<StringColumnPatternYdbFixture>.FunkyDataQueryFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;
    }
}
