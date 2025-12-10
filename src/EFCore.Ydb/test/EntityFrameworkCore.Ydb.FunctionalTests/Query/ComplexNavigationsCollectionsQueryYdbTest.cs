using Microsoft.EntityFrameworkCore.Query;
using Xunit;
using Xunit.Abstractions;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

// TODO: Success rate: 250/300
public class ComplexNavigationsCollectionsQueryYdbTest : ComplexNavigationsCollectionsQueryRelationalTestBase<
    ComplexNavigationsQueryYdbFixture>
{
    public ComplexNavigationsCollectionsQueryYdbTest(
        ComplexNavigationsQueryYdbFixture fixture,
        ITestOutputHelper outputHelper
    ) : base(fixture)
    {
        fixture.TestSqlLoggerFactory.Clear();
        fixture.TestSqlLoggerFactory.SetTestOutputHelper(outputHelper);
    }

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_inside_subquery(bool async) => base.Include_inside_subquery(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Complex_query_with_let_collection_projection_FirstOrDefault_with_ToList_on_inner_and_outer(bool async) =>
        base.Complex_query_with_let_collection_projection_FirstOrDefault_with_ToList_on_inner_and_outer(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Complex_query_with_let_collection_projection_FirstOrDefault(bool async) =>
        base.Complex_query_with_let_collection_projection_FirstOrDefault(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Take_Select_collection_Take(bool async) => base.Take_Select_collection_Take(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Skip_Take_Select_collection_Skip_Take(bool async) =>
        base.Skip_Take_Select_collection_Skip_Take(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_collection_with_multiple_orderbys_methodcall(bool async) =>
        base.Include_collection_with_multiple_orderbys_methodcall(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_collection_with_multiple_orderbys_complex(bool async) =>
        base.Include_collection_with_multiple_orderbys_complex(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_collection_multiple_with_filter(bool async) =>
        base.Include_collection_multiple_with_filter(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_collection_multiple_with_filter_EF_Property(bool async) =>
        base.Include_collection_multiple_with_filter_EF_Property(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Filtered_include_after_different_filtered_include_different_level(bool async) =>
        base.Filtered_include_after_different_filtered_include_different_level(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Filtered_include_same_filter_set_on_same_navigation_twice_followed_by_ThenIncludes(bool async) =>
        base.Filtered_include_same_filter_set_on_same_navigation_twice_followed_by_ThenIncludes(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Filtered_include_multiple_multi_level_includes_with_first_level_using_filter_include_on_one_of_the_chains_only(
            bool async
        ) =>
        base
            .Filtered_include_multiple_multi_level_includes_with_first_level_using_filter_include_on_one_of_the_chains_only(
                async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Filtered_include_and_non_filtered_include_followed_by_then_include_on_same_navigation(bool async) =>
        base.Filtered_include_and_non_filtered_include_followed_by_then_include_on_same_navigation(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Filtered_include_complex_three_level_with_middle_having_filter1(bool async) =>
        base.Filtered_include_complex_three_level_with_middle_having_filter1(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Filtered_include_complex_three_level_with_middle_having_filter2(bool async) =>
        base.Filtered_include_complex_three_level_with_middle_having_filter2(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Filtered_include_context_accessed_inside_filter_correlated(bool async) =>
        base.Filtered_include_context_accessed_inside_filter_correlated(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Filtered_include_outer_parameter_used_inside_filter(bool async) =>
        base.Filtered_include_outer_parameter_used_inside_filter(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Filtered_include_Take_with_another_Take_on_top_level(bool async) =>
        base.Filtered_include_Take_with_another_Take_on_top_level(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Filtered_include_Skip_Take_with_another_Skip_Take_on_top_level(bool async) =>
        base.Filtered_include_Skip_Take_with_another_Skip_Take_on_top_level(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Filtered_include_with_Take_without_order_by_followed_by_ThenInclude_and_FirstOrDefault_on_top_level(
            bool async
        ) =>
        base.Filtered_include_with_Take_without_order_by_followed_by_ThenInclude_and_FirstOrDefault_on_top_level(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Filtered_include_with_Take_without_order_by_followed_by_ThenInclude_and_unordered_Take_on_top_level(
            bool async
        ) =>
        base.Filtered_include_with_Take_without_order_by_followed_by_ThenInclude_and_unordered_Take_on_top_level(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Skip_Take_Distinct_on_grouping_element(bool async) =>
        base.Skip_Take_Distinct_on_grouping_element(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Skip_Take_on_grouping_element_with_collection_include(bool async) =>
        base.Skip_Take_on_grouping_element_with_collection_include(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Skip_Take_on_grouping_element_with_reference_include(bool async) =>
        base.Skip_Take_on_grouping_element_with_reference_include(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Skip_Take_on_grouping_element_inside_collection_projection(bool async) =>
        base.Skip_Take_on_grouping_element_inside_collection_projection(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        SelectMany_with_predicate_and_DefaultIfEmpty_projecting_root_collection_element_and_another_collection(
            bool async
        ) =>
        base.SelectMany_with_predicate_and_DefaultIfEmpty_projecting_root_collection_element_and_another_collection(
            async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Complex_query_issue_21665(bool async) => base.Complex_query_issue_21665(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Projecting_collection_after_optional_reference_correlated_with_parent(bool async) =>
        base.Projecting_collection_after_optional_reference_correlated_with_parent(async);

    [ConditionalTheory(Skip = "TODO: Try to fix correlated subquery")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Projecting_collection_with_group_by_after_optional_reference_correlated_with_parent(bool async) =>
        base.Projecting_collection_with_group_by_after_optional_reference_correlated_with_parent(async);

    [ConditionalTheory(Skip = "OrderBy parameter not included in SELECT")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Skip_Take_on_grouping_element_into_non_entity(bool async) =>
        base.Skip_Take_on_grouping_element_into_non_entity(async);

    [Theory]
    [MemberData(nameof(IsAsyncData))]
    public override Task SelectMany_with_navigation_and_Distinct_projecting_columns_including_join_key(bool async) =>
        base.SelectMany_with_navigation_and_Distinct_projecting_columns_including_join_key(async);
}
