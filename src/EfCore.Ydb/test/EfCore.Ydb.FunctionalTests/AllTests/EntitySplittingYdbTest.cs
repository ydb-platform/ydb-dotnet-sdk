using EfCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace EfCore.Ydb.FunctionalTests.AllTests;

public class EntitySplittingYdbTest(ITestOutputHelper testOutputHelper) : EntitySplittingTestBase(testOutputHelper)
{
    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;
}
