using Microsoft.EntityFrameworkCore.BulkUpdates;
using Xunit.Abstractions;

namespace EfCore.Ydb.FunctionalTests.AllTests.BulkUpdates;

// TODO: Key columns are not specified :c
internal class TPTInheritanceBulkUpdatesYdbTest(
    TPTInheritanceBulkUpdatesYdbFixture fixture,
    ITestOutputHelper testOutputHelper
) : TPTInheritanceBulkUpdatesTestBase<TPTInheritanceBulkUpdatesYdbFixture>(fixture, testOutputHelper);
