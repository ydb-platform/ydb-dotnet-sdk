using Microsoft.EntityFrameworkCore.BulkUpdates;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

// Internal harness; public provider tests expose only the supported Microsoft scenarios.
internal class NorthwindBulkUpdatesYdbTest(
    NorthwindBulkUpdatesYdbFixture<NoopModelCustomizer> fixture,
    ITestOutputHelper testOutputHelper
) : NorthwindBulkUpdatesRelationalTestBase<NorthwindBulkUpdatesYdbFixture<NoopModelCustomizer>>(
    fixture,
    testOutputHelper
);
