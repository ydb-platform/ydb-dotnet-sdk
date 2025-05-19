using Microsoft.EntityFrameworkCore.BulkUpdates;
using Xunit.Abstractions;

namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

// TODO: Primary key required for ydb tables
internal class TPHInheritanceBulkUpdatesYdbTest(
    TPHInheritanceBulkUpdatesYdbFixture fixture,
    ITestOutputHelper testOutputHelper)
    : TPHInheritanceBulkUpdatesTestBase<TPHInheritanceBulkUpdatesYdbFixture>(fixture, testOutputHelper);
