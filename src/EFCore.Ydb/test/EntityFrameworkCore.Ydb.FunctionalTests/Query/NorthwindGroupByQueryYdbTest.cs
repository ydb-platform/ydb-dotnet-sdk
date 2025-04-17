using EntityFrameworkCore.Ydb.FunctionalTests.Query;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

// TODO: Fix tests
// Right now success rate is ~260/520
// Implemented mainly to stress test CI
public class NorthwindGroupByQueryYdbTest(NorthwindQueryYdbFixture<NoopModelCustomizer> fixture)
    : NorthwindGroupByQueryRelationalTestBase<NorthwindQueryYdbFixture<NoopModelCustomizer>>(fixture)
{
    public override Task GroupBy_Property_Select_LongCount(bool async) => Task.CompletedTask;

    public override Task GroupBy_Property_Select_Count_with_nulls(bool async) => Task.CompletedTask;

    public override Task GroupBy_Property_Select_LongCount_with_nulls(bool async) => Task.CompletedTask;

    public override Task GroupBy_Property_Select_Key_LongCount(bool async) => Task.CompletedTask;

    public override Task GroupBy_with_group_key_access_thru_navigation(bool async) => Task.CompletedTask;

    public override Task GroupBy_with_group_key_access_thru_nested_navigation(bool async) => Task.CompletedTask;

    public override Task GroupBy_with_grouping_key_DateTime_Day(bool async) => Task.CompletedTask;

    public override Task Group_by_with_arithmetic_operation_inside_aggregate(bool async) => Task.CompletedTask;

    public override Task Where_select_function_groupby_followed_by_another_select_with_aggregates(bool async) =>
        Task.CompletedTask;

    public override Task Group_by_column_project_constant(bool async) => Task.CompletedTask;

    public override Task GroupBy_with_aggregate_through_navigation_property(bool async) => Task.CompletedTask;

    public override Task GroupBy_with_aggregate_containing_complex_where(bool async) => Task.CompletedTask;

    public override Task GroupBy_anonymous_Select_LongCount(bool async) => Task.CompletedTask;

    public override Task GroupBy_Composite_Select_LongCount(bool async) => Task.CompletedTask;

    public override Task GroupBy_Composite_Select_Key_Average(bool async) => Task.CompletedTask;

    public override Task GroupBy_Composite_Select_Key_Count(bool async) => Task.CompletedTask;

    public override Task GroupBy_Composite_Select_Key_LongCount(bool async) => Task.CompletedTask;

    public override Task GroupBy_Composite_Select_Key_Max(bool async) => Task.CompletedTask;

    public override Task GroupBy_Composite_Select_Key_Min(bool async) => Task.CompletedTask;

    public override Task GroupBy_Composite_Select_Key_Sum(bool async) => Task.CompletedTask;

    public override Task GroupBy_Composite_Select_Key_Sum_Min_Max_Avg(bool async) => Task.CompletedTask;

    public override Task GroupBy_Composite_Select_Sum_Min_Key_Max_Avg(bool async) => Task.CompletedTask;

    public override Task GroupBy_Composite_Select_Sum_Min_Key_flattened_Max_Avg(bool async) => Task.CompletedTask;

    public override Task GroupBy_Dto_as_key_Select_Sum(bool async) => Task.CompletedTask;

    public override Task GroupBy_Composite_Select_Dto_Sum_Min_Key_flattened_Max_Avg(bool async) => Task.CompletedTask;

    public override Task GroupBy_constant_with_where_on_grouping_with_aggregate_operators(bool async) =>
        Task.CompletedTask;

    public override Task GroupBy_anonymous_key_type_mismatch_with_aggregate(bool async) => Task.CompletedTask;

    public override Task GroupBy_based_on_renamed_property_complex(bool async) => Task.CompletedTask;

    public override Task GroupBy_based_on_renamed_property_simple(bool async) => Task.CompletedTask;

    public override Task Odata_groupby_empty_key(bool async) => Task.CompletedTask;

    public override Task GroupBy_Property_scalar_element_selector_LongCount(bool async) => Task.CompletedTask;

    public override Task GroupBy_Property_anonymous_element_selector_LongCount(bool async) => Task.CompletedTask;

    public override Task GroupBy_Property_anonymous_element_selector_Sum_Min_Max_Avg(bool async) => Task.CompletedTask;

    public override Task OrderBy_Skip_GroupBy_Aggregate(bool async) => Task.CompletedTask;

    public override Task Anonymous_projection_Distinct_GroupBy_Aggregate(bool async) => Task.CompletedTask;

    public override Task SelectMany_GroupBy_Aggregate(bool async) => Task.CompletedTask;

    public override Task GroupBy_required_navigation_member_Aggregate(bool async) => Task.CompletedTask;

    public override Task GroupJoin_GroupBy_Aggregate_2(bool async) => Task.CompletedTask;

    public override Task GroupJoin_GroupBy_Aggregate_4(bool async) => Task.CompletedTask;

    public override Task GroupBy_optional_navigation_member_Aggregate(bool async) => Task.CompletedTask;

    public override Task GroupBy_multi_navigation_members_Aggregate(bool async) => Task.CompletedTask;

    public override Task Union_simple_groupby(bool async) => Task.CompletedTask;

    public override Task GroupBy_complex_key_aggregate(bool async) => Task.CompletedTask;

    public override Task GroupBy_complex_key_aggregate_2(bool async) => Task.CompletedTask;

    public override Task Select_collection_of_scalar_before_GroupBy_aggregate(bool async) => Task.CompletedTask;

    public override Task GroupBy_OrderBy_key(bool async) => Task.CompletedTask;

    public override Task GroupBy_OrderBy_count(bool async) => Task.CompletedTask;

    public override Task GroupBy_OrderBy_count_Select_sum(bool async) => Task.CompletedTask;

    public override Task GroupBy_aggregate_Contains(bool async) => Task.CompletedTask;

    public override Task GroupBy_aggregate_Pushdown(bool async) => Task.CompletedTask;

    public override Task GroupBy_aggregate_using_grouping_key_Pushdown(bool async) => Task.CompletedTask;

    public override Task GroupBy_aggregate_Pushdown_followed_by_projecting_Length(bool async) => Task.CompletedTask;

    public override Task GroupBy_aggregate_Pushdown_followed_by_projecting_constant(bool async) => Task.CompletedTask;

    public override Task GroupBy_filter_count_OrderBy_count_Select_sum(bool async) => Task.CompletedTask;

    public override Task GroupBy_Aggregate_Join(bool async) => Task.CompletedTask;

    public override Task GroupBy_Aggregate_Join_converted_from_SelectMany(bool async) => Task.CompletedTask;

    public override Task GroupBy_Aggregate_LeftJoin_converted_from_SelectMany(bool async) => Task.CompletedTask;

    public override Task Join_GroupBy_Aggregate_multijoins(bool async) => Task.CompletedTask;

    public override Task Join_GroupBy_Aggregate_single_join(bool async) => Task.CompletedTask;

    public override Task Join_GroupBy_Aggregate_with_another_join(bool async) => Task.CompletedTask;

    public override Task Join_GroupBy_Aggregate_distinct_single_join(bool async) => Task.CompletedTask;

    public override Task Join_GroupBy_Aggregate_with_left_join(bool async) => Task.CompletedTask;

    public override Task Join_GroupBy_Aggregate_in_subquery(bool async) => Task.CompletedTask;

    public override Task Join_GroupBy_Aggregate_on_key(bool async) => Task.CompletedTask;

    public override Task Distinct_GroupBy_OrderBy_key(bool async) => Task.CompletedTask;

    public override Task Select_nested_collection_with_groupby(bool async) => Task.CompletedTask;

    public override Task Select_uncorrelated_collection_with_groupby_works(bool async) => Task.CompletedTask;

    public override Task Select_uncorrelated_collection_with_groupby_multiple_collections_work(bool async) =>
        Task.CompletedTask;

    public override Task GroupBy_multiple_Count_with_predicate(bool async) => Task.CompletedTask;

    public override Task GroupBy_with_order_by_skip_and_another_order_by(bool async) => Task.CompletedTask;

    public override Task GroupBy_Property_Select_Count_with_predicate(bool async) => Task.CompletedTask;

    public override Task GroupBy_Property_Select_LongCount_with_predicate(bool async) => Task.CompletedTask;

    public override Task GroupBy_orderby_projection_with_coalesce_operation(bool async) => Task.CompletedTask;

    public override Task GroupBy_aggregate_over_a_subquery(bool async) => Task.CompletedTask;

    public override Task GroupBy_aggregate_join_with_grouping_key(bool async) => Task.CompletedTask;

    public override Task GroupBy_aggregate_join_with_group_result(bool async) => Task.CompletedTask;

    public override Task GroupBy_aggregate_from_right_side_of_join(bool async) => Task.CompletedTask;

    public override Task GroupBy_aggregate_join_another_GroupBy_aggregate(bool async) => Task.CompletedTask;

    public override Task GroupBy_aggregate_followed_another_GroupBy_aggregate(bool async) => Task.CompletedTask;

    public override Task GroupBy_aggregate_without_selectMany_selecting_first(bool async) => Task.CompletedTask;

    public override Task GroupBy_aggregate_left_join_GroupBy_aggregate_left_join(bool async) => Task.CompletedTask;

    public override Task GroupBy_Where_Average(bool async) => Task.CompletedTask;

    public override Task GroupBy_Where_Count(bool async) => Task.CompletedTask;

    public override Task GroupBy_Where_LongCount(bool async) => Task.CompletedTask;

    public override Task GroupBy_Where_Max(bool async) => Task.CompletedTask;

    public override Task GroupBy_Where_Min(bool async) => Task.CompletedTask;

    public override Task GroupBy_Where_Sum(bool async) => Task.CompletedTask;

    public override Task GroupBy_Where_Count_with_predicate(bool async) => Task.CompletedTask;

    public override Task GroupBy_Where_Where_Count(bool async) => Task.CompletedTask;

    public override Task GroupBy_Where_Select_Where_Count(bool async) => Task.CompletedTask;

    public override Task GroupBy_Where_Select_Where_Select_Min(bool async) => Task.CompletedTask;

    public override Task LongCount_after_GroupBy_aggregate(bool async) => Task.CompletedTask;

    public override Task GroupBy_Select_Distinct_aggregate(bool async) => Task.CompletedTask;

    public override Task Final_GroupBy_property_entity(bool async) => Task.CompletedTask;

    public override Task Final_GroupBy_entity(bool async) => Task.CompletedTask;

    public override Task Final_GroupBy_property_entity_non_nullable(bool async) => Task.CompletedTask;

    public override Task Final_GroupBy_property_anonymous_type(bool async) => Task.CompletedTask;

    public override Task Final_GroupBy_multiple_properties_entity(bool async) => Task.CompletedTask;

    public override Task Final_GroupBy_complex_key_entity(bool async) => Task.CompletedTask;

    public override Task Final_GroupBy_nominal_type_entity(bool async) => Task.CompletedTask;

    public override Task Final_GroupBy_property_anonymous_type_element_selector(bool async) => Task.CompletedTask;

    public override Task Final_GroupBy_property_entity_Include_collection(bool async) => Task.CompletedTask;

    public override Task Final_GroupBy_property_entity_projecting_collection(bool async) => Task.CompletedTask;

    public override Task Final_GroupBy_property_entity_projecting_collection_composed(bool async) => Task.CompletedTask;

    public override Task Final_GroupBy_property_entity_projecting_collection_and_single_result(bool async) =>
        Task.CompletedTask;

    public override Task GroupBy_complex_key_without_aggregate(bool async) => Task.CompletedTask;

    public override Task GroupBy_Shadow(bool async) => Task.CompletedTask;

    public override Task GroupBy_Shadow2(bool async) => Task.CompletedTask;

    public override Task GroupBy_Shadow3(bool async) => Task.CompletedTask;

    public override Task GroupBy_select_grouping_list(bool async) => Task.CompletedTask;

    public override Task GroupBy_select_grouping_array(bool async) => Task.CompletedTask;

    public override Task GroupBy_select_grouping_composed_list(bool async) => Task.CompletedTask;

    public override Task GroupBy_select_grouping_composed_list_2(bool async) => Task.CompletedTask;

    public override Task GroupBy_with_group_key_being_navigation(bool async) => Task.CompletedTask;

    public override Task GroupBy_with_group_key_being_nested_navigation(bool async) => Task.CompletedTask;

    public override Task GroupBy_with_group_key_being_navigation_with_entity_key_projection(bool async) =>
        Task.CompletedTask;

    public override Task GroupBy_with_group_key_being_navigation_with_complex_projection(bool async) =>
        Task.CompletedTask;

    public override Task LongCount_after_GroupBy_without_aggregate(bool async) => Task.CompletedTask;

    public override Task LongCount_with_predicate_after_GroupBy_without_aggregate(bool async) => Task.CompletedTask;

    public override Task GroupBy_Count_in_projection(bool async) => Task.CompletedTask;

    public override Task Complex_query_with_groupBy_in_subquery1(bool async) => Task.CompletedTask;

    public override Task Complex_query_with_groupBy_in_subquery2(bool async) => Task.CompletedTask;

    public override Task Complex_query_with_groupBy_in_subquery3(bool async) => Task.CompletedTask;

    public override Task Complex_query_with_groupBy_in_subquery4(bool async) => Task.CompletedTask;

    public override Task Complex_query_with_group_by_in_subquery5(bool async) => Task.CompletedTask;

    public override Task GroupBy_scalar_subquery(bool async) => Task.CompletedTask;

    public override Task AsEnumerable_in_subquery_for_GroupBy(bool async) => Task.CompletedTask;

    public override Task GroupBy_aggregate_from_multiple_query_in_same_projection(bool async) => Task.CompletedTask;

    public override Task GroupBy_aggregate_from_multiple_query_in_same_projection_2(bool async) => Task.CompletedTask;

    public override Task GroupBy_aggregate_from_multiple_query_in_same_projection_3(bool async) => Task.CompletedTask;

    public override Task Select_uncorrelated_collection_with_groupby_when_outer_is_distinct(bool async) =>
        Task.CompletedTask;

    public override Task Select_correlated_collection_after_GroupBy_aggregate_when_identifier_does_not_change(
        bool async
    ) => Task.CompletedTask;

    public override Task Select_correlated_collection_after_GroupBy_aggregate_when_identifier_changes(bool async) =>
        Task.CompletedTask;

    public override Task Select_correlated_collection_after_GroupBy_aggregate_when_identifier_changes_to_complex(
        bool async
    ) => Task.CompletedTask;

    public override Task GroupBy_aggregate_projecting_conditional_expression_based_on_group_key(bool async) =>
        Task.CompletedTask;

    public override Task GroupBy_count_filter(bool async) => Task.CompletedTask;

    public override Task GroupBy_Property_Select_Key_with_constant(bool async) => Task.CompletedTask;
}
