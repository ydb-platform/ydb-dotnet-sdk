using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class YdbServiceCollectionExtensionsTest()
    : RelationalServiceCollectionExtensionsTestBase(YdbTestHelpers.Instance);
