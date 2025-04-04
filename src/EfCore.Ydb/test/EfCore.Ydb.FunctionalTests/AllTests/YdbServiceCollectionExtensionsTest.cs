using EfCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;

namespace EfCore.Ydb.FunctionalTests.AllTests;

public class YdbServiceCollectionExtensionsTest()
    : RelationalServiceCollectionExtensionsTestBase(YdbTestHelpers.Instance);
