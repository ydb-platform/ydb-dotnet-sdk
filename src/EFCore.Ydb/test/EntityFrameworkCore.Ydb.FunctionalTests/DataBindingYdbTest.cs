using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

/// <summary>
/// Tests for data binding support in YDB provider.
/// </summary>
public class DataBindingYdbTest : DataBindingTestBase<F1YdbFixture>
{
    public DataBindingYdbTest(F1YdbFixture fixture)
        : base(fixture)
    {
    }

    // Most data binding tests should work with YDB
    // Any specific limitations will be discovered during test runs
}
