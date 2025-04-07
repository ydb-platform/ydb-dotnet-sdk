using EfCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EfCore.Ydb.FunctionalTests.Migrations;

public class YdbMigrationsInfrastructureTest(YdbMigrationsInfrastructureTest.YdbMigrationsInfrastructureFixture fixture)
    // : MigrationsInfrastructureTestBase<YdbMigrationsInfrastructureTest.YdbMigrationsInfrastructureFixture>(fixture)
{
    public class YdbMigrationsInfrastructureFixture : MigrationsInfrastructureFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory => YdbTestStoreFactory.Instance;
    }
    //
    // public override void Can_diff_against_2_2_model()
    // {
    // }
    //
    // public override void Can_diff_against_3_0_ASP_NET_Identity_model()
    // {
    // }
    //
    // public override void Can_diff_against_2_2_ASP_NET_Identity_model()
    // {
    // }
    //
    // public override void Can_diff_against_2_1_ASP_NET_Identity_model()
    // {
    // }
    //
    // protected override Task ExecuteSqlAsync(string value) =>
    //     ((YdbTestStore)Fixture.TestStore).ExecuteNonQueryAsync(value);
}
