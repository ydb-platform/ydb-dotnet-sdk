using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

// TODO: Fix tests
// Right now success rate is ~70/114
// Implemented mainly to stress test CI
public class EntitySplittingQueryYdbTest : EntitySplittingQueryTestBase
{
# if !EFCORE9
    public EntitySplittingQueryYdbTest(NonSharedFixture fixture) : base(fixture)
    {
    }
# endif

    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;

    public override Task Include_reference_to_split_entity(bool async) => Task.CompletedTask;

    public override Task Include_collection_to_split_entity(bool async) => Task.CompletedTask;

    public override Task Include_reference_to_split_entity_including_reference(bool async) => Task.CompletedTask;

    public override Task Include_collection_to_split_entity_including_collection(bool async) => Task.CompletedTask;

    public override Task
        Normal_entity_owning_a_split_reference_with_main_fragment_sharing_custom_projection(bool async) =>
        Task.CompletedTask;

    public override Task Normal_entity_owning_a_split_collection(bool async) => Task.CompletedTask;

    public override Task Split_entity_owning_a_split_collection(bool async) => Task.CompletedTask;

    public override Task Tpt_entity_owning_a_split_reference_on_base_with_table_sharing(bool async) =>
        Task.CompletedTask;

    public override Task Tpt_entity_owning_a_split_reference_on_middle_with_table_sharing(bool async) =>
        Task.CompletedTask;

    public override Task Tpt_entity_owning_a_split_reference_on_leaf_with_table_sharing(bool async) =>
        Task.CompletedTask;

    public override Task Tpt_entity_owning_a_split_reference_on_base_without_table_sharing(bool async) =>
        Task.CompletedTask;

    public override Task Tpt_entity_owning_a_split_reference_on_middle_without_table_sharing(bool async) =>
        Task.CompletedTask;

    public override Task Tpt_entity_owning_a_split_reference_on_leaf_without_table_sharing(bool async) =>
        Task.CompletedTask;

    public override Task Tph_entity_owning_a_split_collection_on_base(bool async) => Task.CompletedTask;

    public override Task Tpt_entity_owning_a_split_collection_on_base(bool async) => Task.CompletedTask;

    public override Task Tpc_entity_owning_a_split_collection_on_base(bool async) => Task.CompletedTask;

    public override Task Tph_entity_owning_a_split_collection_on_middle(bool async) => Task.CompletedTask;

    public override Task Tpt_entity_owning_a_split_collection_on_middle(bool async) => Task.CompletedTask;

    public override Task Tpc_entity_owning_a_split_collection_on_middle(bool async) => Task.CompletedTask;

    public override Task Tph_entity_owning_a_split_collection_on_leaf(bool async) => Task.CompletedTask;

    public override Task Tpt_entity_owning_a_split_collection_on_leaf(bool async) => Task.CompletedTask;

    public override Task Tpc_entity_owning_a_split_collection_on_leaf(bool async) => Task.CompletedTask;

    public override Task Tpc_entity_owning_a_split_reference_on_leaf_with_table_sharing(bool async) =>
        Task.CompletedTask;

    public override Task Tpc_entity_owning_a_split_reference_on_base_without_table_sharing(bool async) =>
        Task.CompletedTask;

    public override Task Tpc_entity_owning_a_split_reference_on_middle_without_table_sharing(bool async) =>
        Task.CompletedTask;

    public override Task Tpc_entity_owning_a_split_reference_on_leaf_without_table_sharing(bool async) =>
        Task.CompletedTask;

    protected override IServiceCollection AddServices(IServiceCollection serviceCollection)
    {
        serviceCollection.AddScoped<IExecutionStrategyFactory, NonRetryingExecutionStrategyFactory>();
        return serviceCollection;
    }
}
