using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Migrations;

public class YdbMigrationsInfrastructureTest(YdbMigrationsInfrastructureTest.YdbMigrationsInfrastructureFixture fixture)
    : MigrationsInfrastructureTestBase<YdbMigrationsInfrastructureTest.YdbMigrationsInfrastructureFixture>(fixture)
{
    public class YdbMigrationsInfrastructureFixture : MigrationsInfrastructureFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory => YdbTestStoreFactory.Instance;

        protected override IServiceCollection AddServices(IServiceCollection serviceCollection)
        {
            serviceCollection.AddScoped<IExecutionStrategyFactory, NonRetryingExecutionStrategyFactory>();
            return base.AddServices(serviceCollection);
        }
    }

    protected override void GiveMeSomeTime(DbContext db)
    {
    }

    protected override Task GiveMeSomeTimeAsync(DbContext db) => Task.CompletedTask;

    [ConditionalFact(Skip = "TODO")]
    public override void Can_diff_against_2_2_model()
    {
    }

    [ConditionalFact(Skip = "TODO")]
    public override void Can_diff_against_3_0_ASP_NET_Identity_model()
    {
    }

    [ConditionalFact(Skip = "TODO")]
    public override void Can_diff_against_2_2_ASP_NET_Identity_model()
    {
    }

    [ConditionalFact(Skip = "TODO")]
    public override void Can_diff_against_2_1_ASP_NET_Identity_model()
    {
    }

    [ConditionalFact(Skip = "TODO")]
    public override void Can_apply_all_migrations() => base.Can_apply_all_migrations();

    [ConditionalFact(Skip = "TODO")]
    public override Task Can_apply_all_migrations_async() => base.Can_apply_all_migrations_async();

    [ConditionalFact(Skip = "TODO")]
    public override void Can_apply_range_of_migrations() => base.Can_apply_range_of_migrations();

    [ConditionalFact(Skip = "TODO")]
    public override void Can_apply_second_migration_in_parallel() => base.Can_apply_second_migration_in_parallel();

    [ConditionalFact(Skip = "TODO")]
    public override Task Can_apply_second_migration_in_parallel_async() =>
        base.Can_apply_second_migration_in_parallel_async();

    [ConditionalFact(Skip = "TODO")]
    public override void Can_apply_two_migrations_in_transaction() => base.Can_apply_two_migrations_in_transaction();

    [ConditionalFact(Skip = "TODO")]
    public override Task Can_apply_two_migrations_in_transaction_async() =>
        base.Can_apply_two_migrations_in_transaction_async();

    [ConditionalFact(Skip = "TODO")]
    public override Task Can_generate_idempotent_up_and_down_scripts() =>
        base.Can_generate_idempotent_up_and_down_scripts();

    [ConditionalFact(Skip = "TODO")]
    public override Task Can_generate_idempotent_up_and_down_scripts_noTransactions() =>
        base.Can_generate_idempotent_up_and_down_scripts_noTransactions();

    [ConditionalFact(Skip = "TODO")]
    public override Task Can_generate_one_up_and_down_script() => base.Can_generate_one_up_and_down_script();

    [ConditionalFact(Skip = "TODO")]
    public override Task Can_generate_up_and_down_script_using_names() =>
        base.Can_generate_up_and_down_script_using_names();

    [ConditionalFact(Skip = "TODO")]
    public override Task Can_generate_up_and_down_scripts() => base.Can_generate_up_and_down_scripts();

    [ConditionalFact(Skip = "TODO")]
    public override Task Can_generate_up_and_down_scripts_noTransactions() =>
        base.Can_generate_up_and_down_scripts_noTransactions();

    [ConditionalFact(Skip = "TODO")]
    public override void Can_revert_all_migrations() => base.Can_revert_all_migrations();

    [ConditionalFact(Skip = "TODO")]
    public override void Can_revert_one_migrations() => base.Can_revert_one_migrations();

    public override void Can_get_active_provider()
    {
        base.Can_get_active_provider();

        Assert.Equal("EntityFrameworkCore.Ydb", ActiveProvider);
    }

    [ConditionalFact(Skip = "TODO")]
    public override void Can_apply_one_migration_in_parallel() => base.Can_apply_one_migration_in_parallel();

    [ConditionalFact(Skip = "TODO")]
    public override Task Can_apply_one_migration_in_parallel_async() => base.Can_apply_one_migration_in_parallel_async();

    protected override Task ExecuteSqlAsync(string value) =>
        ((YdbTestStore)Fixture.TestStore).ExecuteNonQueryAsync(value);
}
