using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;
using Xunit.Sdk;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

// TODO: Fix translation for INNER JOIN:
// Join cannot contain `>`
// ```
// SELECT ...
// FROM ... AS `l`
// INNER JOIN (
//     SELECT ...
//     FROM .. AS `l1`
// ) AS `l2` ON `l`.`Id` = `l2`.`Key` AND `l2`.`Sum` > 10
// ```
public class ComplexNavigationsQueryYdbTest
    : ComplexNavigationsQueryRelationalTestBase<ComplexNavigationsQueryYdbFixture>
{
    public ComplexNavigationsQueryYdbTest(ComplexNavigationsQueryYdbFixture fixture)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
    }

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Simple_level1_level2_GroupBy_Having_Count(bool async) =>
        base.Simple_level1_level2_GroupBy_Having_Count(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Optional_navigation_inside_nested_method_call_translated_to_join(bool async) =>
        base.Optional_navigation_inside_nested_method_call_translated_to_join(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task
        Method_call_on_optional_navigation_translates_to_null_conditional_properly_for_arguments(bool async) =>
        base.Method_call_on_optional_navigation_translates_to_null_conditional_properly_for_arguments(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task
        Optional_navigation_inside_method_call_translated_to_join_keeps_original_nullability(bool async) =>
        base.Optional_navigation_inside_method_call_translated_to_join_keeps_original_nullability(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task
        Optional_navigation_inside_nested_method_call_translated_to_join_keeps_original_nullability(bool async) =>
        base.Optional_navigation_inside_nested_method_call_translated_to_join_keeps_original_nullability(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task
        Optional_navigation_inside_nested_method_call_translated_to_join_keeps_original_nullability_also_for_arguments(
            bool async
        ) =>
        base
            .Optional_navigation_inside_nested_method_call_translated_to_join_keeps_original_nullability_also_for_arguments(
                async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Query_source_materialization_bug_4547(bool async) =>
        base.Query_source_materialization_bug_4547(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Where_navigation_property_to_collection(bool async) =>
        base.Where_navigation_property_to_collection(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Where_navigation_property_to_collection2(bool async) =>
        base.Where_navigation_property_to_collection2(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Where_navigation_property_to_collection_of_original_entity_type(bool async) =>
        base.Where_navigation_property_to_collection_of_original_entity_type(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Correlated_subquery_doesnt_project_unnecessary_columns_in_top_level(bool async) =>
        base.Correlated_subquery_doesnt_project_unnecessary_columns_in_top_level(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Correlated_subquery_doesnt_project_unnecessary_columns_in_top_level_join(bool async) =>
        base.Correlated_subquery_doesnt_project_unnecessary_columns_in_top_level_join(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task SelectMany_where_with_subquery(bool async) => base.SelectMany_where_with_subquery(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Select_join_with_key_selector_being_a_subquery(bool async) =>
        base.Select_join_with_key_selector_being_a_subquery(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Contains_with_subquery_optional_navigation_and_constant_item(bool async) =>
        base.Contains_with_subquery_optional_navigation_and_constant_item(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Contains_with_subquery_optional_navigation_scalar_distinct_and_constant_item(bool async) =>
        base.Contains_with_subquery_optional_navigation_scalar_distinct_and_constant_item(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Required_navigation_on_a_subquery_with_First_in_projection(bool async) =>
        base.Required_navigation_on_a_subquery_with_First_in_projection(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Required_navigation_on_a_subquery_with_complex_projection_and_First(bool async) =>
        base.Required_navigation_on_a_subquery_with_complex_projection_and_First(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Required_navigation_on_a_subquery_with_First_in_predicate(bool async) =>
        base.Required_navigation_on_a_subquery_with_First_in_predicate(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task GroupJoin_in_subquery_with_client_result_operator(bool async) =>
        base.GroupJoin_in_subquery_with_client_result_operator(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task GroupJoin_in_subquery_with_client_projection(bool async) =>
        base.GroupJoin_in_subquery_with_client_projection(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task GroupJoin_in_subquery_with_client_projection_nested1(bool async) =>
        base.GroupJoin_in_subquery_with_client_projection_nested1(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task GroupJoin_in_subquery_with_client_projection_nested2(bool async) =>
        base.GroupJoin_in_subquery_with_client_projection_nested2(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Explicit_GroupJoin_in_subquery_with_scalar_result_operator(bool async) =>
        base.Explicit_GroupJoin_in_subquery_with_scalar_result_operator(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task
        Explicit_GroupJoin_in_subquery_with_multiple_result_operator_distinct_count_materializes_main_clause(
            bool async
        ) =>
        base.Explicit_GroupJoin_in_subquery_with_multiple_result_operator_distinct_count_materializes_main_clause(
            async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task
        Join_condition_optimizations_applied_correctly_when_anonymous_type_with_single_property(bool async) =>
        base.Join_condition_optimizations_applied_correctly_when_anonymous_type_with_single_property(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task
        Join_condition_optimizations_applied_correctly_when_anonymous_type_with_multiple_properties(bool async) =>
        base.Join_condition_optimizations_applied_correctly_when_anonymous_type_with_multiple_properties(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Navigations_compared_to_each_other3(bool async) =>
        base.Navigations_compared_to_each_other3(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Navigations_compared_to_each_other4(bool async) =>
        base.Navigations_compared_to_each_other4(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Navigations_compared_to_each_other5(bool async) =>
        base.Navigations_compared_to_each_other5(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_with_client_eval_and_navigation1(bool async) =>
        base.Select_subquery_with_client_eval_and_navigation1(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_with_client_eval_and_navigation2(bool async) =>
        base.Select_subquery_with_client_eval_and_navigation2(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_with_client_eval_and_multi_level_navigation(bool async) =>
        base.Select_subquery_with_client_eval_and_multi_level_navigation(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Member_doesnt_get_pushed_down_into_subquery_with_result_operator(bool async) =>
        base.Member_doesnt_get_pushed_down_into_subquery_with_result_operator(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Subquery_with_Distinct_Skip_FirstOrDefault_without_OrderBy(bool async) =>
        base.Subquery_with_Distinct_Skip_FirstOrDefault_without_OrderBy(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Project_collection_navigation_count(bool async) =>
        base.Project_collection_navigation_count(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Member_pushdown_chain_3_levels_deep(bool async) =>
        base.Member_pushdown_chain_3_levels_deep(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Member_pushdown_with_collection_navigation_in_the_middle(bool async) =>
        base.Member_pushdown_with_collection_navigation_in_the_middle(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Member_pushdown_with_multiple_collections(bool async) =>
        base.Member_pushdown_with_multiple_collections(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task OrderBy_collection_count_ThenBy_reference_navigation(bool async) =>
        base.OrderBy_collection_count_ThenBy_reference_navigation(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Sum_with_filter_with_include_selector_cast_using_as(bool async) =>
        base.Sum_with_filter_with_include_selector_cast_using_as(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task SelectMany_with_outside_reference_to_joined_table_correctly_translated_to_apply(bool async) =>
        base.SelectMany_with_outside_reference_to_joined_table_correctly_translated_to_apply(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Contains_over_optional_navigation_with_null_column(bool async) =>
        base.Contains_over_optional_navigation_with_null_column(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Contains_over_optional_navigation_with_null_entity_reference(bool async) =>
        base.Contains_over_optional_navigation_with_null_entity_reference(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Element_selector_with_coalesce_repeated_in_aggregate(bool async) =>
        base.Element_selector_with_coalesce_repeated_in_aggregate(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Nested_object_constructed_from_group_key_properties(bool async) =>
        base.Nested_object_constructed_from_group_key_properties(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Let_let_contains_from_outer_let(bool async) => base.Let_let_contains_from_outer_let(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Composite_key_join_on_groupby_aggregate_projecting_only_grouping_key2(bool async) =>
        base.Composite_key_join_on_groupby_aggregate_projecting_only_grouping_key2(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Collection_FirstOrDefault_property_accesses_in_projection(bool async) =>
        base.Collection_FirstOrDefault_property_accesses_in_projection(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Multiple_collection_FirstOrDefault_followed_by_member_access_in_projection(bool async) =>
        base.Multiple_collection_FirstOrDefault_followed_by_member_access_in_projection(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Prune_does_not_throw_null_ref(bool async) => base.Prune_does_not_throw_null_ref(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Correlated_projection_with_first(bool async) => base.Correlated_projection_with_first(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Max_in_multi_level_nested_subquery(bool async) =>
        base.Max_in_multi_level_nested_subquery(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Multiple_select_many_in_projection(bool async) =>
        base.Multiple_select_many_in_projection(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Single_select_many_in_projection_with_take(bool async) =>
        base.Single_select_many_in_projection_with_take(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Composite_key_join_on_groupby_aggregate_projecting_only_grouping_key(bool async) =>
        base.Composite_key_join_on_groupby_aggregate_projecting_only_grouping_key(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task GroupJoin_client_method_in_OrderBy(bool async)
        => base.GroupJoin_client_method_in_OrderBy(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Nested_SelectMany_correlated_with_join_table_correctly_translated_to_apply(bool async)
        => base.Nested_SelectMany_correlated_with_join_table_correctly_translated_to_apply(async);

    [ConditionalTheory(Skip = "TODO: Fix translation for INNER JOIN"), MemberData(nameof(IsAsyncData))]
    public override Task Join_with_result_selector_returning_queryable_throws_validation_error(bool async) =>
        base.Join_with_result_selector_returning_queryable_throws_validation_error(async);
}
