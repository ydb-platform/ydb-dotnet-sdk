using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class EntitySplittingYdbTest(
# if !EFCORE9
    NonSharedFixture nonSharedFixture,
#endif
    ITestOutputHelper testOutputHelper) :
    EntitySplittingTestBase(
# if !EFCORE9
        nonSharedFixture,
#endif
        testOutputHelper)
{
    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;
}
