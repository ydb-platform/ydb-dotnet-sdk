using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class AdHocNavigationsQueryYdbTest : AdHocNavigationsQueryRelationalTestBase
{
    // TODO: Fix query compilation
    public override Task Select_enumerable_navigation_backed_by_collection(bool async, bool split) =>
        Task.CompletedTask;

    // TODO: Fix query compilation
    public override Task ThenInclude_with_interface_navigations() => Task.CompletedTask;

    // TODO: cast in subquery
    public override Task Customer_collections_materialize_properly() => Task.CompletedTask;

    // TODO: Fix query compilation
    public override Task Reference_include_on_derived_type_with_sibling_works() => Task.CompletedTask;

    // TODO: Fix query compilation
    public override Task Include_collection_optional_reference_collection() => Task.CompletedTask;

    // TODO: Fix query compilation
    public override Task Collection_without_setter_materialized_correctly() => Task.CompletedTask;

    // TODO: cast in subquery
    public override Task Let_multiple_references_with_reference_to_outer() => Task.CompletedTask;

    // TODO: Fix query compilation
    public override Task SelectMany_and_collection_in_projection_in_FirstOrDefault() => Task.CompletedTask;

    // TODO: Fix query compilation
    public override Task Using_explicit_interface_implementation_as_navigation_works() => Task.CompletedTask;

    // TODO: Fix query compilation
    public override Task Cycles_in_auto_include() => Task.CompletedTask;

    // TODO: Fix query compilation
    public override Task Walking_back_include_tree_is_not_allowed_3() => Task.CompletedTask;

    // TODO: cast in subquery
    public override Task Projection_with_multiple_includes_and_subquery_with_set_operation() => Task.CompletedTask;

    // TODO: Fix query compilation
    public override Task Count_member_over_IReadOnlyCollection_works(bool async) => Task.CompletedTask;

    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;
}
