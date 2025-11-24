using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

/// <summary>
/// Ad-hoc tests for miscellaneous query scenarios in YDB provider.
/// Note: Some query patterns are skipped due to YDB server limitations.
/// </summary>
public class AdHocMiscellaneousQueryYdbTest : AdHocMiscellaneousQueryRelationalTestBase
{
    protected override ITestStoreFactory TestStoreFactory => YdbTestStoreFactory.Instance;

    protected override Task Seed2951(Context2951 context)
    {
        // YDB-specific seeding for context 2951 - using default implementation
        return Task.CompletedTask;
    }
}
