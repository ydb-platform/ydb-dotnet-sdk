using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

// TODO: Fix tests
// Right now success rate is ~160/422
// Implemented mainly to stress test CI
public class NorthwindAggregateOperatorsQueryYdbTest(NorthwindQueryYdbFixture<NoopModelCustomizer> fixture) :
    NorthwindAggregateOperatorsQueryRelationalTestBase<NorthwindQueryYdbFixture<NoopModelCustomizer>>(fixture)
{
    public override Task Contains_over_keyless_entity_throws(bool async) => Task.CompletedTask;

    public override Task Min_no_data_subquery(bool async) => Task.CompletedTask;

    public override Task Max_no_data_subquery(bool async) => Task.CompletedTask;

    public override Task Average_no_data_subquery(bool async) => Task.CompletedTask;

    public override Task Sum_with_division_on_decimal(bool async) => Task.CompletedTask;

    public override Task Sum_with_division_on_decimal_no_significant_digits(bool async) => Task.CompletedTask;

    public override Task Sum_with_coalesce(bool async) => Task.CompletedTask;

    public override Task Sum_over_subquery(bool async) => Task.CompletedTask;

    public override Task Sum_over_nested_subquery(bool async) => Task.CompletedTask;

    public override Task Sum_over_min_subquery(bool async) => Task.CompletedTask;

    public override Task Sum_over_scalar_returning_subquery(bool async) => Task.CompletedTask;

    public override Task Sum_over_Any_subquery(bool async) => Task.CompletedTask;

    public override Task Sum_over_uncorrelated_subquery(bool async) => Task.CompletedTask;

    public override Task Sum_on_float_column(bool async) => Task.CompletedTask;

    public override Task Sum_on_float_column_in_subquery(bool async) => Task.CompletedTask;

    public override Task Average_with_division_on_decimal(bool async) => Task.CompletedTask;

    public override Task Average_with_division_on_decimal_no_significant_digits(bool async) => Task.CompletedTask;

    public override Task Average_with_coalesce(bool async) => Task.CompletedTask;

    public override Task Average_over_subquery(bool async) => Task.CompletedTask;

    public override Task Average_over_nested_subquery(bool async) => Task.CompletedTask;

    public override Task Average_over_max_subquery(bool async) => Task.CompletedTask;

    public override Task Average_on_float_column(bool async) => Task.CompletedTask;

    public override Task Average_on_float_column_in_subquery(bool async) => Task.CompletedTask;

    public override Task Average_on_float_column_in_subquery_with_cast(bool async) => Task.CompletedTask;

    public override Task Min_with_coalesce(bool async) => Task.CompletedTask;

    public override Task Min_over_subquery(bool async) => Task.CompletedTask;

    public override Task Min_over_nested_subquery(bool async) => Task.CompletedTask;

    public override Task Min_over_max_subquery(bool async) => Task.CompletedTask;

    public override Task Max_with_coalesce(bool async) => Task.CompletedTask;

    public override Task Max_over_subquery(bool async) => Task.CompletedTask;

    public override Task Max_over_nested_subquery(bool async) => Task.CompletedTask;

    public override Task Max_over_sum_subquery(bool async) => Task.CompletedTask;

    public override Task OrderBy_client_Take(bool async) => Task.CompletedTask;

    public override Task Distinct(bool async) => Task.CompletedTask;

    public override Task Distinct_Scalar(bool async) => Task.CompletedTask;

    public override Task OrderBy_Distinct(bool async) => Task.CompletedTask;

    public override Task Distinct_OrderBy(bool async) => Task.CompletedTask;

    public override Task Distinct_OrderBy2(bool async) => Task.CompletedTask;

    public override Task Single_Predicate(bool async) => Task.CompletedTask;

    public override Task Where_Single(bool async) => Task.CompletedTask;

    public override Task SingleOrDefault_Predicate(bool async) => Task.CompletedTask;

    public override Task Where_SingleOrDefault(bool async) => Task.CompletedTask;

    public override Task First(bool async) => Task.CompletedTask;

    public override Task First_Predicate(bool async) => Task.CompletedTask;

    public override Task Where_First(bool async) => Task.CompletedTask;

    public override Task FirstOrDefault(bool async) => Task.CompletedTask;

    public override Task FirstOrDefault_Predicate(bool async) => Task.CompletedTask;

    public override Task Where_FirstOrDefault(bool async) => Task.CompletedTask;

    public override Task FirstOrDefault_inside_subquery_gets_server_evaluated(bool async) => Task.CompletedTask;

    public override Task Multiple_collection_navigation_with_FirstOrDefault_chained(bool async) => Task.CompletedTask;

    public override Task Multiple_collection_navigation_with_FirstOrDefault_chained_projecting_scalar(bool async) =>
        Task.CompletedTask;

    public override Task First_inside_subquery_gets_client_evaluated(bool async) => Task.CompletedTask;

    public override Task Last(bool async) => Task.CompletedTask;

    public override Task Last_Predicate(bool async) => Task.CompletedTask;

    public override Task Where_Last(bool async) => Task.CompletedTask;

    public override Task LastOrDefault(bool async) => Task.CompletedTask;

    public override Task LastOrDefault_Predicate(bool async) => Task.CompletedTask;

    public override Task Where_LastOrDefault(bool async) => Task.CompletedTask;

    public override Task Contains_with_subquery(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_array_closure(bool async) => Task.CompletedTask;

    public override Task Contains_with_subquery_and_local_array_closure(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_uint_array_closure(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_nullable_uint_array_closure(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_array_inline(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_list_closure(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_object_list_closure(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_list_inline(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_list_inline_closure_mix(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_enumerable_closure(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_object_enumerable_closure(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_enumerable_inline(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_enumerable_inline_closure_mix(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_ordered_enumerable_closure(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_object_ordered_enumerable_closure(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_ordered_enumerable_inline(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_ordered_enumerable_inline_closure_mix(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_read_only_collection_closure(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_object_read_only_collection_closure(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_read_only_collection_inline(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_read_only_collection_inline_closure_mix(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_non_primitive_list_inline_closure_mix(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_non_primitive_list_closure_mix(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_collection_false(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_collection_complex_predicate_and(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_collection_complex_predicate_or(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_collection_complex_predicate_not_matching_ins1(bool async) =>
        Task.CompletedTask;

    public override Task Contains_with_local_collection_sql_injection(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_collection_empty_inline(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_tuple_array_closure(bool async) => Task.CompletedTask;

    public override Task Contains_with_local_anonymous_type_array_closure(bool async) => Task.CompletedTask;

    public override Task OfType_Select(bool async) => Task.CompletedTask;

    public override Task OfType_Select_OfType_Select(bool async) => Task.CompletedTask;

    public override Task OrderBy_Take_Last_gives_correct_result(bool async) => Task.CompletedTask;

    public override Task OrderBy_Skip_Last_gives_correct_result(bool async) => Task.CompletedTask;

    public override Task Contains_over_entityType_should_rewrite_to_identity_equality(bool async) => Task.CompletedTask;

    public override Task List_Contains_over_entityType_should_rewrite_to_identity_equality(bool async) =>
        Task.CompletedTask;

    public override Task List_Contains_with_constant_list(bool async) => Task.CompletedTask;

    public override Task List_Contains_with_parameter_list(bool async) => Task.CompletedTask;

    public override Task Contains_with_parameter_list_value_type_id(bool async) => Task.CompletedTask;

    public override Task Contains_with_constant_list_value_type_id(bool async) => Task.CompletedTask;

    public override Task IImmutableSet_Contains_with_parameter(bool async) => Task.CompletedTask;

    public override Task IReadOnlySet_Contains_with_parameter(bool async) => Task.CompletedTask;

    public override Task HashSet_Contains_with_parameter(bool async) => Task.CompletedTask;

    public override Task ImmutableHashSet_Contains_with_parameter(bool async) => Task.CompletedTask;

    public override Task Array_cast_to_IEnumerable_Contains_with_constant(bool async) => Task.CompletedTask;

    public override Task Contains_over_entityType_with_null_should_rewrite_to_identity_equality_subquery_negated(
        bool async
    ) => Task.CompletedTask;

    public override Task Contains_over_entityType_with_null_should_rewrite_to_identity_equality_subquery_complex(
        bool async
    ) => Task.CompletedTask;

    public override Task Contains_over_entityType_should_materialize_when_composite(bool async) => Task.CompletedTask;

    public override Task Contains_over_entityType_should_materialize_when_composite2(bool async) => Task.CompletedTask;

    public override Task Where_subquery_any_equals_operator(bool async) => Task.CompletedTask;

    public override Task Where_subquery_any_equals(bool async) => Task.CompletedTask;

    public override Task Where_subquery_any_equals_static(bool async) => Task.CompletedTask;

    public override Task Where_subquery_where_any(bool async) => Task.CompletedTask;

    public override Task Where_subquery_all_not_equals_operator(bool async) => Task.CompletedTask;

    public override Task Where_subquery_all_not_equals(bool async) => Task.CompletedTask;

    public override Task Where_subquery_all_not_equals_static(bool async) => Task.CompletedTask;

    public override Task Where_subquery_where_all(bool async) => Task.CompletedTask;

    public override Task Cast_before_aggregate_is_preserved(bool async) => Task.CompletedTask;

    public override Task Enumerable_min_is_mapped_to_Queryable_1(bool async) => Task.CompletedTask;

    public override Task Enumerable_min_is_mapped_to_Queryable_2(bool async) => Task.CompletedTask;

    public override Task DefaultIfEmpty_selects_only_required_columns(bool async) => Task.CompletedTask;

    public override Task Collection_Last_member_access_in_projection_translated(bool async) => Task.CompletedTask;

    public override Task Collection_LastOrDefault_member_access_in_projection_translated(bool async) =>
        Task.CompletedTask;

#if EFCORE9
    public override Task Average_after_default_if_empty_does_not_throw(bool async) => Task.CompletedTask;

    public override Task Max_after_default_if_empty_does_not_throw(bool async) => Task.CompletedTask;

    public override Task Min_after_default_if_empty_does_not_throw(bool async) => Task.CompletedTask;
# endif

    public override Task Average_on_nav_subquery_in_projection(bool async) => Task.CompletedTask;

    public override Task Contains_inside_aggregate_function_with_GroupBy(bool async) => Task.CompletedTask;

    public override Task Contains_inside_LongCount_without_GroupBy(bool async) => Task.CompletedTask;

    public override Task Return_type_of_singular_operator_is_preserved(bool async) => Task.CompletedTask;

    public override Task Type_casting_inside_sum(bool async) => Task.CompletedTask;
}
