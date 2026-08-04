using Microsoft.EntityFrameworkCore.BulkUpdates;
using Xunit.Abstractions;

namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

// Internal harness; public provider tests expose only the supported Microsoft scenarios.
internal class NorthwindBulkUpdatesYdbTest(
    NorthwindBulkUpdatesYdbFixture<YdbNorthwindModelCustomizer> fixture,
    ITestOutputHelper testOutputHelper
) : NorthwindBulkUpdatesRelationalTestBase<NorthwindBulkUpdatesYdbFixture<YdbNorthwindModelCustomizer>>(
    fixture,
    testOutputHelper
);
