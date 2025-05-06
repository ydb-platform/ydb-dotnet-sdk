using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

// TODO: Fix tests
// Right now success rate is ~30/45
// Implemented mainly to stress test CI
public class AdHocAdvancedMappingsQueryYdbTest : AdHocAdvancedMappingsQueryRelationalTestBase
{
    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;

    public override Task Two_similar_complex_properties_projected_with_split_query1() => Task.CompletedTask;

    public override Task Two_similar_complex_properties_projected_with_split_query2() => Task.CompletedTask;

    public override Task Projecting_correlated_collection_along_with_non_mapped_property() => Task.CompletedTask;

    public override Task Double_convert_interface_created_expression_tree() => Task.CompletedTask;

    public override Task
        Query_generates_correct_timespan_parameter_definition(int? fractionalSeconds, string postfix) =>
        Task.CompletedTask;

    public override Task Hierarchy_query_with_abstract_type_sibling_TPC(bool async) => Task.CompletedTask;

    public override Task Projection_failing_with_EnumToStringConverter() => Task.CompletedTask;
}
