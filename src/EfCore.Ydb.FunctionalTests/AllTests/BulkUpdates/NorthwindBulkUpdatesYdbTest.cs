using Microsoft.EntityFrameworkCore.BulkUpdates;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace EfCore.Ydb.FunctionalTests.AllTests.BulkUpdates;

// TODO: Await Norhhwind
class NorthwindBulkUpdatesYdbTest(
    NorthwindBulkUpdatesYdbFixture<NoopModelCustomizer> fixture,
    ITestOutputHelper testOutputHelper)
    : NorthwindBulkUpdatesRelationalTestBase<NorthwindBulkUpdatesYdbFixture<NoopModelCustomizer>>(fixture, testOutputHelper)
{
}