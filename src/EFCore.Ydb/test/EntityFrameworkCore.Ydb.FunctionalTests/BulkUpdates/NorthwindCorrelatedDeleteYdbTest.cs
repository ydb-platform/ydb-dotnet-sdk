using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;
using Xunit.Abstractions;
using static EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities.SharedTestMethods;

namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

public sealed class NorthwindCorrelatedDeleteYdbTest :
    IClassFixture<NorthwindBulkUpdatesYdbFixture<NoopModelCustomizer>>
{
    private readonly NorthwindBulkUpdatesYdbFixture<NoopModelCustomizer> _fixture;
    private readonly NorthwindBulkUpdatesYdbTest _microsoftTests;

    public NorthwindCorrelatedDeleteYdbTest(
        NorthwindBulkUpdatesYdbFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
    {
        _fixture = fixture;
        _microsoftTests = new NorthwindBulkUpdatesYdbTest(fixture, testOutputHelper);
    }

    [ConditionalTheory]
    [InlineData(false)]
    [InlineData(true)]
    public Task Delete_Where_using_navigation_2(bool async)
        => AssertYdb(
            _microsoftTests.Delete_Where_using_navigation_2,
            _fixture.TestSqlLoggerFactory,
            async,
            """
            DELETE FROM `Order_Details` ON SELECT `o`.`OrderID` AS `OrderID`, `o`.`ProductID` AS `ProductID`
            FROM `Order_Details` AS `o`
            INNER JOIN `Orders` AS `o0` ON `o`.`OrderID` = `o0`.`OrderID`
            LEFT JOIN `Customers` AS `c` ON `o0`.`CustomerID` = `c`.`CustomerID`
            WHERE `c`.`CustomerID` LIKE 'F%'u
            """);
}
