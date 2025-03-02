using Microsoft.EntityFrameworkCore.BulkUpdates;
using Xunit.Abstractions;

namespace EfCore.Ydb.FunctionalTests.AllTests.BulkUpdates;

// TODO: Primary key required for ydb tables
public class TphInheritanceBulkUpdatesYdbTest(
    TPHInheritanceBulkUpdatesYdbFixture fixture,
    ITestOutputHelper testOutputHelper)
    : TPHInheritanceBulkUpdatesTestBase<TPHInheritanceBulkUpdatesYdbFixture>(fixture, testOutputHelper)
    {
}