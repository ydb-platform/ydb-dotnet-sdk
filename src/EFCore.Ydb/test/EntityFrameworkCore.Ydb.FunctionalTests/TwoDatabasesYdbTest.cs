using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

// TODO: Right now in tests we can access only one database
public class TwoDatabasesYdbTest(ComplexTypesTrackingYdbTest.YdbFixture fixture)
    : TwoDatabasesTestBase(fixture), IClassFixture<YdbFixture>
{
    [ConditionalFact(Skip = "Explained in TODO")]
    public override void Can_query_from_one_connection_and_save_changes_to_another() =>
        base.Can_query_from_one_connection_and_save_changes_to_another();

    [ConditionalFact(Skip = "Explained in TODO")]
    public override void Can_query_from_one_connection_string_and_save_changes_to_another() =>
        base.Can_query_from_one_connection_string_and_save_changes_to_another();

    [ConditionalTheory(Skip = "Explained in TODO")]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public override void Can_set_connection_string_in_interceptor(
        bool withConnectionString, bool withNullConnectionString
    ) => base.Can_set_connection_string_in_interceptor(withConnectionString, withNullConnectionString);

    protected override DbContextOptionsBuilder CreateTestOptions(
        DbContextOptionsBuilder optionsBuilder,
        bool withConnectionString = false,
        bool withNullConnectionString = false
    ) => throw new NotImplementedException();

    protected override TwoDatabasesWithDataContext CreateBackingContext(string databaseName)
        => throw new NotImplementedException();

    protected override string DummyConnectionString
        => throw new NotImplementedException();
}
