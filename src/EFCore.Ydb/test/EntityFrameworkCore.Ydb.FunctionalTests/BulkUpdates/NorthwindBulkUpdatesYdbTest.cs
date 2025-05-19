using Microsoft.EntityFrameworkCore.BulkUpdates;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

// TODO: Await Norhhwind
internal class NorthwindBulkUpdatesYdbTest(
    NorthwindBulkUpdatesYdbFixture<NoopModelCustomizer> fixture,
    ITestOutputHelper testOutputHelper
) : NorthwindBulkUpdatesRelationalTestBase<NorthwindBulkUpdatesYdbFixture<NoopModelCustomizer>>(
    fixture,
    testOutputHelper
);
