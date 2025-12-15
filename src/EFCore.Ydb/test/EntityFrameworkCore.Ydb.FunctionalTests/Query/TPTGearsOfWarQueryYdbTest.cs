using Microsoft.EntityFrameworkCore.Query;
using Xunit;
using Xunit.Abstractions;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

// TODO: Correlated subqueries right now are not supported in YDB
public class TPTGearsOfWarQueryYdbTest : TPTGearsOfWarQueryRelationalTestBase<TPTGearsOfWarQueryYdbFixture>
{
    // ReSharper disable once UnusedParameter.Local
    public TPTGearsOfWarQueryYdbTest(TPTGearsOfWarQueryYdbFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Correlated_collection_with_distinct_not_projecting_identifier_column_also_projecting_complex_expressions(
            bool async
        ) =>
        base.Correlated_collection_with_distinct_not_projecting_identifier_column_also_projecting_complex_expressions(
            async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_is_lifted_from_main_from_clause_of_SelectMany(bool async) =>
        base.Subquery_is_lifted_from_main_from_clause_of_SelectMany(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_containing_join_gets_lifted_clashing_names(bool async) =>
        base.Subquery_containing_join_gets_lifted_clashing_names(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Optional_Navigation_Null_Coalesce_To_Clr_Type(bool async) =>
        base.Optional_Navigation_Null_Coalesce_To_Clr_Type(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task String_concat_on_various_types(bool async) => base.String_concat_on_various_types(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collection_after_distinct_3_levels_without_original_identifiers(bool async) =>
        base.Correlated_collection_after_distinct_3_levels_without_original_identifiers(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_with_nested_navigation_in_order_by(bool async) =>
        base.Include_with_nested_navigation_in_order_by(async);

#if EFCORE9
    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_bitwise_and_enum(bool async) => base.Where_bitwise_and_enum(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_bitwise_and_integral(bool async) => base.Where_bitwise_and_integral(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_bitwise_and_nullable_enum_with_constant(bool async) =>
        base.Where_bitwise_and_nullable_enum_with_constant(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_bitwise_and_nullable_enum_with_null_constant(bool async) =>
        base.Where_bitwise_and_nullable_enum_with_null_constant(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_bitwise_and_nullable_enum_with_non_nullable_parameter(bool async) =>
        base.Where_bitwise_and_nullable_enum_with_non_nullable_parameter(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_bitwise_and_nullable_enum_with_nullable_parameter(bool async) =>
        base.Where_bitwise_and_nullable_enum_with_nullable_parameter(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_bitwise_or_enum(bool async) => base.Where_bitwise_or_enum(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Bitwise_projects_values_in_select(bool async) => base.Bitwise_projects_values_in_select(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_enum_has_flag(bool async) => base.Where_enum_has_flag(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_enum_has_flag_subquery(bool async) => base.Where_enum_has_flag_subquery(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_enum_has_flag_subquery_with_pushdown(bool async) =>
        base.Where_enum_has_flag_subquery_with_pushdown(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_enum_has_flag_subquery_client_eval(bool async) =>
        base.Where_enum_has_flag_subquery_client_eval(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_enum_has_flag_with_non_nullable_parameter(bool async) =>
        base.Where_enum_has_flag_with_non_nullable_parameter(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_has_flag_with_nullable_parameter(bool async) =>
        base.Where_has_flag_with_nullable_parameter(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_enum_has_flag(bool async) => base.Select_enum_has_flag(async);
#endif

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_count_subquery_without_collision(bool async) =>
        base.Where_count_subquery_without_collision(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_any_subquery_without_collision(bool async) =>
        base.Where_any_subquery_without_collision(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_null_parameter(bool async) => base.Select_null_parameter(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Null_propagation_optimization4(bool async) => base.Null_propagation_optimization4(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Null_propagation_optimization5(bool async) => base.Null_propagation_optimization5(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Null_propagation_optimization6(bool async) => base.Null_propagation_optimization6(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_null_propagation_negative3(bool async) => base.Select_null_propagation_negative3(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_null_propagation_negative4(bool async) => base.Select_null_propagation_negative4(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_null_propagation_negative5(bool async) => base.Select_null_propagation_negative5(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_null_propagation_works_for_multiple_navigations_with_composite_keys(bool async) =>
        base.Select_null_propagation_works_for_multiple_navigations_with_composite_keys(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_boolean(bool async) => base.Where_subquery_boolean(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_boolean_with_pushdown(bool async) =>
        base.Where_subquery_boolean_with_pushdown(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_distinct_firstordefault_boolean(bool async) =>
        base.Where_subquery_distinct_firstordefault_boolean(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_distinct_firstordefault_boolean_with_pushdown(bool async) =>
        base.Where_subquery_distinct_firstordefault_boolean_with_pushdown(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_distinct_first_boolean(bool async) =>
        base.Where_subquery_distinct_first_boolean(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_distinct_singleordefault_boolean1(bool async) =>
        base.Where_subquery_distinct_singleordefault_boolean1(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_distinct_singleordefault_boolean2(bool async) =>
        base.Where_subquery_distinct_singleordefault_boolean2(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_distinct_singleordefault_boolean_with_pushdown(bool async) =>
        base.Where_subquery_distinct_singleordefault_boolean_with_pushdown(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_distinct_lastordefault_boolean(bool async) =>
        base.Where_subquery_distinct_lastordefault_boolean(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_distinct_last_boolean(bool async) =>
        base.Where_subquery_distinct_last_boolean(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_distinct_orderby_firstordefault_boolean(bool async) =>
        base.Where_subquery_distinct_orderby_firstordefault_boolean(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_distinct_orderby_firstordefault_boolean_with_pushdown(bool async) =>
        base.Where_subquery_distinct_orderby_firstordefault_boolean_with_pushdown(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_union_firstordefault_boolean(bool async) =>
        base.Where_subquery_union_firstordefault_boolean(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_join_firstordefault_boolean(bool async) =>
        base.Where_subquery_join_firstordefault_boolean(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_left_join_firstordefault_boolean(bool async) =>
        base.Where_subquery_left_join_firstordefault_boolean(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_concat_firstordefault_boolean(bool async) =>
        base.Where_subquery_concat_firstordefault_boolean(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_navigation_with_concat_and_count(bool async) =>
        base.Select_navigation_with_concat_and_count(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Concat_with_collection_navigations(bool async) =>
        base.Concat_with_collection_navigations(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Union_with_collection_navigations(bool async) => base.Union_with_collection_navigations(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_distinct_firstordefault(bool async) =>
        base.Select_subquery_distinct_firstordefault(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Non_unicode_string_literals_is_used_for_non_unicode_column_with_subquery(bool async) =>
        base.Non_unicode_string_literals_is_used_for_non_unicode_column_with_subquery(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Optional_navigation_type_compensation_works_with_list_initializers(bool async) =>
        base.Optional_navigation_type_compensation_works_with_list_initializers(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Optional_navigation_type_compensation_works_with_orderby(bool async) =>
        base.Optional_navigation_type_compensation_works_with_orderby(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_correlated_filtered_collection_works_with_caching(bool async) =>
        base.Select_correlated_filtered_collection_works_with_caching(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Join_predicate_value_equals_condition(bool async) =>
        base.Join_predicate_value_equals_condition(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Join_predicate_value(bool async) => base.Join_predicate_value(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Join_predicate_condition_equals_condition(bool async) =>
        base.Join_predicate_condition_equals_condition(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Left_join_predicate_value_equals_condition(bool async) =>
        base.Left_join_predicate_value_equals_condition(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Left_join_predicate_value(bool async) => base.Left_join_predicate_value(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Left_join_predicate_condition_equals_condition(bool async) =>
        base.Left_join_predicate_condition_equals_condition(async);

#if EFCORE9
    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_datetimeoffset_now(bool async) => base.Where_datetimeoffset_now(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_datetimeoffset_utcnow(bool async) => base.Where_datetimeoffset_utcnow(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_datetimeoffset_date_component(bool async) =>
        base.Where_datetimeoffset_date_component(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_datetimeoffset_year_component(bool async) =>
        base.Where_datetimeoffset_year_component(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_datetimeoffset_month_component(bool async) =>
        base.Where_datetimeoffset_month_component(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_datetimeoffset_dayofyear_component(bool async) =>
        base.Where_datetimeoffset_dayofyear_component(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_datetimeoffset_day_component(bool async) =>
        base.Where_datetimeoffset_day_component(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_datetimeoffset_hour_component(bool async) =>
        base.Where_datetimeoffset_hour_component(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_datetimeoffset_minute_component(bool async) =>
        base.Where_datetimeoffset_minute_component(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_datetimeoffset_second_component(bool async) =>
        base.Where_datetimeoffset_second_component(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_datetimeoffset_millisecond_component(bool async) =>
        base.Where_datetimeoffset_millisecond_component(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DateTimeOffset_DateAdd_AddYears(bool async) => base.DateTimeOffset_DateAdd_AddYears(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DateTimeOffset_DateAdd_AddMonths(bool async) => base.DateTimeOffset_DateAdd_AddMonths(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DateTimeOffset_DateAdd_AddDays(bool async) => base.DateTimeOffset_DateAdd_AddDays(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DateTimeOffset_DateAdd_AddHours(bool async) => base.DateTimeOffset_DateAdd_AddHours(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DateTimeOffset_DateAdd_AddMinutes(bool async) => base.DateTimeOffset_DateAdd_AddMinutes(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DateTimeOffset_DateAdd_AddSeconds(bool async) => base.DateTimeOffset_DateAdd_AddSeconds(async);
#endif

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Any_with_optional_navigation_as_subquery_predicate_is_translated_to_sql(bool async) =>
        base.Any_with_optional_navigation_as_subquery_predicate_is_translated_to_sql(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_created_by_include_gets_lifted_nested(bool async) =>
        base.Subquery_created_by_include_gets_lifted_nested(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_is_lifted_from_additional_from_clause(bool async) =>
        base.Subquery_is_lifted_from_additional_from_clause(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Skip_with_orderby_followed_by_orderBy_is_pushed_down(bool async) =>
        base.Skip_with_orderby_followed_by_orderBy_is_pushed_down(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Take_without_orderby_followed_by_orderBy_is_pushed_down1(bool async) =>
        base.Take_without_orderby_followed_by_orderBy_is_pushed_down1(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Take_without_orderby_followed_by_orderBy_is_pushed_down2(bool async) =>
        base.Take_without_orderby_followed_by_orderBy_is_pushed_down2(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Collection_navigation_access_on_derived_entity_using_cast(bool async) =>
        base.Collection_navigation_access_on_derived_entity_using_cast(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Collection_navigation_access_on_derived_entity_using_cast_in_SelectMany(bool async) =>
        base.Collection_navigation_access_on_derived_entity_using_cast_in_SelectMany(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Comparing_two_collection_navigations_composite_key(bool async) =>
        base.Comparing_two_collection_navigations_composite_key(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Comparing_entities_using_Equals_inheritance(bool async) =>
        base.Comparing_entities_using_Equals_inheritance(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Optional_navigation_with_collection_composite_key(bool async) =>
        base.Optional_navigation_with_collection_composite_key(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Project_collection_navigation_with_inheritance2(bool async) =>
        base.Project_collection_navigation_with_inheritance2(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Project_collection_navigation_with_inheritance3(bool async) =>
        base.Project_collection_navigation_with_inheritance3(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_reference_on_derived_type_using_string_nested2(bool async) =>
        base.Include_reference_on_derived_type_using_string_nested2(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task ThenInclude_collection_on_derived_after_derived_reference(bool async) =>
        base.ThenInclude_collection_on_derived_after_derived_reference(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Multiple_derived_included_on_one_method(bool async) =>
        base.Multiple_derived_included_on_one_method(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_naked_navigation_with_ToList_followed_by_projecting_count(bool async) =>
        base.Correlated_collections_naked_navigation_with_ToList_followed_by_projecting_count(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_projection_of_collection_thru_navigation(bool async) =>
        base.Correlated_collections_projection_of_collection_thru_navigation(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_nested_with_custom_ordering(bool async) =>
        base.Correlated_collections_nested_with_custom_ordering(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_similar_collection_projected_multiple_times(bool async) =>
        base.Correlated_collections_similar_collection_projected_multiple_times(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_different_collections_projected(bool async) =>
        base.Correlated_collections_different_collections_projected(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Multiple_orderby_with_navigation_expansion_on_one_of_the_order_bys(bool async) =>
        base.Multiple_orderby_with_navigation_expansion_on_one_of_the_order_bys(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Multiple_orderby_with_navigation_expansion_on_one_of_the_order_bys_inside_subquery(bool async) =>
        base.Multiple_orderby_with_navigation_expansion_on_one_of_the_order_bys_inside_subquery(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Multiple_orderby_with_navigation_expansion_on_one_of_the_order_bys_inside_subquery_duplicated_orderings(
            bool async
        ) =>
        base.Multiple_orderby_with_navigation_expansion_on_one_of_the_order_bys_inside_subquery_duplicated_orderings(
            async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Multiple_orderby_with_navigation_expansion_on_one_of_the_order_bys_inside_subquery_complex_orderings(
            bool async
        ) => base.Multiple_orderby_with_navigation_expansion_on_one_of_the_order_bys_inside_subquery_complex_orderings(
        async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_multiple_nested_complex_collections(bool async) =>
        base.Correlated_collections_multiple_nested_complex_collections(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_inner_subquery_selector_references_outer_qsre(bool async) =>
        base.Correlated_collections_inner_subquery_selector_references_outer_qsre(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_inner_subquery_predicate_references_outer_qsre(bool async) =>
        base.Correlated_collections_inner_subquery_predicate_references_outer_qsre(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_nested_inner_subquery_references_outer_qsre_one_level_up(bool async) =>
        base.Correlated_collections_nested_inner_subquery_references_outer_qsre_one_level_up(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_nested_inner_subquery_references_outer_qsre_two_levels_up(bool async) =>
        base.Correlated_collections_nested_inner_subquery_references_outer_qsre_two_levels_up(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_with_Skip(bool async) => base.Correlated_collections_with_Skip(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_with_Take(bool async) => base.Correlated_collections_with_Take(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_with_Distinct(bool async) =>
        base.Correlated_collections_with_Distinct(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_with_FirstOrDefault(bool async) =>
        base.Correlated_collections_with_FirstOrDefault(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_on_left_join_with_null_value(bool async) =>
        base.Correlated_collections_on_left_join_with_null_value(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_left_join_with_self_reference(bool async) =>
        base.Correlated_collections_left_join_with_self_reference(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_deeply_nested_left_join(bool async) =>
        base.Correlated_collections_deeply_nested_left_join(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Correlated_collections_from_left_join_with_additional_elements_projected_of_that_join(bool async) =>
        base.Correlated_collections_from_left_join_with_additional_elements_projected_of_that_join(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collections_with_funky_orderby_complex_scenario2(bool async) =>
        base.Correlated_collections_with_funky_orderby_complex_scenario2(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collection_with_top_level_Last_with_orderby_on_outer(bool async) =>
        base.Correlated_collection_with_top_level_Last_with_orderby_on_outer(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collection_with_top_level_Last_with_order_by_on_inner(bool async) =>
        base.Correlated_collection_with_top_level_Last_with_order_by_on_inner(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_on_derived_type_with_order_by_and_paging(bool async) =>
        base.Include_on_derived_type_with_order_by_and_paging(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Outer_parameter_in_join_key(bool async) => base.Outer_parameter_in_join_key(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Outer_parameter_in_join_key_inner_and_outer(bool async) =>
        base.Outer_parameter_in_join_key_inner_and_outer(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Outer_parameter_in_group_join_with_DefaultIfEmpty(bool async) =>
        base.Outer_parameter_in_group_join_with_DefaultIfEmpty(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Order_by_entity_qsre(bool async) => base.Order_by_entity_qsre(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Order_by_entity_qsre_with_inheritance(bool async) =>
        base.Order_by_entity_qsre_with_inheritance(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Order_by_entity_qsre_composite_key(bool async) =>
        base.Order_by_entity_qsre_composite_key(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Order_by_entity_qsre_with_other_orderbys(bool async) =>
        base.Order_by_entity_qsre_with_other_orderbys(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Project_one_value_type_from_empty_collection(bool async) =>
        base.Project_one_value_type_from_empty_collection(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Project_one_value_type_converted_to_nullable_from_empty_collection(bool async) =>
        base.Project_one_value_type_converted_to_nullable_from_empty_collection(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Filter_on_subquery_projecting_one_value_type_from_empty_collection(bool async) =>
        base.Filter_on_subquery_projecting_one_value_type_from_empty_collection(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_projecting_single_constant_int(bool async) =>
        base.Select_subquery_projecting_single_constant_int(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_projecting_single_constant_string(bool async) =>
        base.Select_subquery_projecting_single_constant_string(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_projecting_single_constant_bool(bool async) =>
        base.Select_subquery_projecting_single_constant_bool(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_collection_OrderBy_aggregate(bool async) =>
        base.Include_collection_OrderBy_aggregate(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_collection_with_complex_OrderBy2(bool async) =>
        base.Include_collection_with_complex_OrderBy2(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_collection_with_complex_OrderBy3(bool async) =>
        base.Include_collection_with_complex_OrderBy3(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collection_with_complex_OrderBy(bool async) =>
        base.Correlated_collection_with_complex_OrderBy(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collection_with_very_complex_order_by(bool async) =>
        base.Correlated_collection_with_very_complex_order_by(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_boolean(bool async) => base.Select_subquery_boolean(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_boolean_with_pushdown(bool async) =>
        base.Select_subquery_boolean_with_pushdown(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_int_with_inside_cast_and_coalesce(bool async) =>
        base.Select_subquery_int_with_inside_cast_and_coalesce(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_int_with_outside_cast_and_coalesce(bool async) =>
        base.Select_subquery_int_with_outside_cast_and_coalesce(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_int_with_pushdown_and_coalesce(bool async) =>
        base.Select_subquery_int_with_pushdown_and_coalesce(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_int_with_pushdown_and_coalesce2(bool async) =>
        base.Select_subquery_int_with_pushdown_and_coalesce2(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_boolean_empty(bool async) => base.Select_subquery_boolean_empty(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_boolean_empty_with_pushdown(bool async) =>
        base.Select_subquery_boolean_empty_with_pushdown(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_distinct_singleordefault_boolean1(bool async) =>
        base.Select_subquery_distinct_singleordefault_boolean1(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_distinct_singleordefault_boolean2(bool async) =>
        base.Select_subquery_distinct_singleordefault_boolean2(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_distinct_singleordefault_boolean_with_pushdown(bool async) =>
        base.Select_subquery_distinct_singleordefault_boolean_with_pushdown(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_distinct_singleordefault_boolean_empty1(bool async) =>
        base.Select_subquery_distinct_singleordefault_boolean_empty1(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_distinct_singleordefault_boolean_empty2(bool async) =>
        base.Select_subquery_distinct_singleordefault_boolean_empty2(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_distinct_singleordefault_boolean_empty_with_pushdown(bool async) =>
        base.Select_subquery_distinct_singleordefault_boolean_empty_with_pushdown(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task GroupBy_Property_Include_Select_LongCount(bool async) =>
        base.GroupBy_Property_Include_Select_LongCount(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task GroupBy_Property_Include_Aggregate_with_anonymous_selector(bool async) =>
        base.GroupBy_Property_Include_Aggregate_with_anonymous_selector(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task OrderBy_same_expression_containing_IsNull_correctly_deduplicates_the_ordering(bool async) =>
        base.OrderBy_same_expression_containing_IsNull_correctly_deduplicates_the_ordering(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task GetValueOrDefault_with_argument_complex(bool async) =>
        base.GetValueOrDefault_with_argument_complex(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Filter_with_complex_predicate_containing_subquery(bool async) =>
        base.Filter_with_complex_predicate_containing_subquery(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Query_with_complex_let_containing_ordering_and_filter_projecting_firstOrDefault_element_of_let(bool async) =>
        base.Query_with_complex_let_containing_ordering_and_filter_projecting_firstOrDefault_element_of_let(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Null_semantics_is_correctly_applied_for_function_comparisons_that_take_arguments_from_optional_navigation(
            bool async
        ) =>
        base.Null_semantics_is_correctly_applied_for_function_comparisons_that_take_arguments_from_optional_navigation(
            async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Null_semantics_is_correctly_applied_for_function_comparisons_that_take_arguments_from_optional_navigation_complex(
            bool async
        ) =>
        base
            .Null_semantics_is_correctly_applied_for_function_comparisons_that_take_arguments_from_optional_navigation_complex(
                async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_contains_on_navigation_with_composite_keys(bool async) =>
        base.Where_contains_on_navigation_with_composite_keys(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_with_complex_order_by(bool async) => base.Include_with_complex_order_by(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Bool_projection_from_subquery_treated_appropriately_in_where(bool async) =>
        base.Bool_projection_from_subquery_treated_appropriately_in_where(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DateTimeOffset_Contains_Less_than_Greater_than(bool async) =>
        base.DateTimeOffset_Contains_Less_than_Greater_than(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DateTimeOffsetNow_minus_timespan(bool async) => base.DateTimeOffsetNow_minus_timespan(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Project_collection_navigation_nested_with_take_composite_key(bool async) =>
        base.Project_collection_navigation_nested_with_take_composite_key(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Project_collection_navigation_nested_composite_key(bool async) =>
        base.Project_collection_navigation_nested_composite_key(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Null_checks_in_correlated_predicate_are_correctly_translated(bool async) =>
        base.Null_checks_in_correlated_predicate_are_correctly_translated(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Accessing_property_of_optional_navigation_in_child_projection_works(bool async) =>
        base.Accessing_property_of_optional_navigation_in_child_projection_works(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Collection_navigation_ofType_filter_works(bool async) =>
        base.Collection_navigation_ofType_filter_works(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Query_reusing_parameter_with_inner_query_expression_doesnt_declare_duplicate_parameter(bool async) =>
        base.Query_reusing_parameter_with_inner_query_expression_doesnt_declare_duplicate_parameter(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Complex_GroupBy_after_set_operator(bool async) =>
        base.Complex_GroupBy_after_set_operator(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Complex_GroupBy_after_set_operator_using_result_selector(bool async) =>
        base.Complex_GroupBy_after_set_operator_using_result_selector(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task GroupBy_with_boolean_groupin_key_thru_navigation_access(bool async) =>
        base.GroupBy_with_boolean_groupin_key_thru_navigation_access(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_with_enum_flags_parameter(bool async) => base.Where_with_enum_flags_parameter(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        FirstOrDefault_navigation_access_entity_equality_in_where_predicate_apply_peneding_selector(bool async) =>
        base.FirstOrDefault_navigation_access_entity_equality_in_where_predicate_apply_peneding_selector(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Conditional_expression_with_test_being_simplified_to_constant_complex(bool isAsync) =>
        base.Conditional_expression_with_test_being_simplified_to_constant_complex(isAsync);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Bitwise_operation_with_non_null_parameter_optimizes_null_checks(bool async) =>
        base.Bitwise_operation_with_non_null_parameter_optimizes_null_checks(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Bitwise_operation_with_null_arguments(bool async) =>
        base.Bitwise_operation_with_null_arguments(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Byte_array_filter_by_length_literal_does_not_cast_on_varbinary_n(bool async) =>
        base.Byte_array_filter_by_length_literal_does_not_cast_on_varbinary_n(async);

#if EFCORE9
    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Byte_array_filter_by_length_literal(bool async) =>
        base.Byte_array_filter_by_length_literal(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Byte_array_filter_by_length_parameter(bool async) =>
        base.Byte_array_filter_by_length_parameter(async);

    [Fact(Skip = "TODO: Fix tests")]
    public override void Byte_array_filter_by_length_parameter_compiled() =>
        base.Byte_array_filter_by_length_parameter_compiled();
#endif

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DateTimeOffset_Date_returns_datetime(bool async) =>
        base.DateTimeOffset_Date_returns_datetime(async);

#if EFCORE9
    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_TimeSpan_Hours(bool async) => base.Where_TimeSpan_Hours(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_TimeSpan_Minutes(bool async) => base.Where_TimeSpan_Minutes(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_TimeSpan_Seconds(bool async) => base.Where_TimeSpan_Seconds(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_TimeSpan_Milliseconds(bool async) => base.Where_TimeSpan_Milliseconds(async);
#endif

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Contains_on_collection_of_nullable_byte_subquery(bool async) =>
        base.Contains_on_collection_of_nullable_byte_subquery(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Subquery_projecting_non_nullable_scalar_contains_non_nullable_value_doesnt_need_null_expansion(bool async) =>
        base.Subquery_projecting_non_nullable_scalar_contains_non_nullable_value_doesnt_need_null_expansion(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Subquery_projecting_non_nullable_scalar_contains_non_nullable_value_doesnt_need_null_expansion_negated(
            bool async
        ) =>
        base.Subquery_projecting_non_nullable_scalar_contains_non_nullable_value_doesnt_need_null_expansion_negated(
            async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_projecting_nullable_scalar_contains_nullable_value_needs_null_expansion(bool async) =>
        base.Subquery_projecting_nullable_scalar_contains_nullable_value_needs_null_expansion(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Subquery_projecting_nullable_scalar_contains_nullable_value_needs_null_expansion_negated(bool async) =>
        base.Subquery_projecting_nullable_scalar_contains_nullable_value_needs_null_expansion_negated(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Enum_flags_closure_typed_as_underlying_type_generates_correct_parameter_type(bool async) =>
        base.Enum_flags_closure_typed_as_underlying_type_generates_correct_parameter_type(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Enum_flags_closure_typed_as_different_type_generates_correct_parameter_type(bool async) =>
        base.Enum_flags_closure_typed_as_different_type_generates_correct_parameter_type(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Constant_enum_with_same_underlying_value_as_previously_parameterized_int(bool async) =>
        base.Constant_enum_with_same_underlying_value_as_previously_parameterized_int(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task SelectMany_predicate_with_non_equality_comparison_converted_to_inner_join(bool async) =>
        base.SelectMany_predicate_with_non_equality_comparison_converted_to_inner_join(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        SelectMany_predicate_with_non_equality_comparison_DefaultIfEmpty_converted_to_left_join(bool async) =>
        base.SelectMany_predicate_with_non_equality_comparison_DefaultIfEmpty_converted_to_left_join(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        SelectMany_predicate_after_navigation_with_non_equality_comparison_DefaultIfEmpty_converted_to_left_join(
            bool async
        ) =>
        base.SelectMany_predicate_after_navigation_with_non_equality_comparison_DefaultIfEmpty_converted_to_left_join(
            async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task SelectMany_without_result_selector_and_non_equality_comparison_converted_to_join(bool async) =>
        base.SelectMany_without_result_selector_and_non_equality_comparison_converted_to_join(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Filtered_collection_projection_with_order_comparison_predicate_converted_to_join(bool async) =>
        base.Filtered_collection_projection_with_order_comparison_predicate_converted_to_join(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Filtered_collection_projection_with_order_comparison_predicate_converted_to_join2(bool async) =>
        base.Filtered_collection_projection_with_order_comparison_predicate_converted_to_join2(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Filtered_collection_projection_with_order_comparison_predicate_converted_to_join3(bool async) =>
        base.Filtered_collection_projection_with_order_comparison_predicate_converted_to_join3(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        SelectMany_predicate_with_non_equality_comparison_with_Take_doesnt_convert_to_join(bool async) =>
        base.SelectMany_predicate_with_non_equality_comparison_with_Take_doesnt_convert_to_join(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task FirstOrDefault_over_int_compared_to_zero(bool async) =>
        base.FirstOrDefault_over_int_compared_to_zero(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collection_with_inner_collection_references_element_two_levels_up(bool async) =>
        base.Correlated_collection_with_inner_collection_references_element_two_levels_up(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task FirstOrDefault_on_empty_collection_of_DateTime_in_subquery(bool async) =>
        base.FirstOrDefault_on_empty_collection_of_DateTime_in_subquery(async);

#if EFCORE9
    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task First_on_byte_array(bool async) => base.First_on_byte_array(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Array_access_on_byte_array(bool async) => base.Array_access_on_byte_array(async);
#endif

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Projecting_property_converted_to_nullable_with_function_call(bool async) =>
        base.Projecting_property_converted_to_nullable_with_function_call(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Projecting_property_converted_to_nullable_into_element_init(bool async) =>
        base.Projecting_property_converted_to_nullable_into_element_init(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Projecting_property_converted_to_nullable_into_member_assignment(bool async) =>
        base.Projecting_property_converted_to_nullable_into_member_assignment(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Projecting_property_converted_to_nullable_into_new_array(bool async) =>
        base.Projecting_property_converted_to_nullable_into_new_array(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Projecting_property_converted_to_nullable_into_member_access(bool async) =>
        base.Projecting_property_converted_to_nullable_into_member_access(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Projecting_property_converted_to_nullable_and_use_it_in_order_by(bool async) =>
        base.Projecting_property_converted_to_nullable_and_use_it_in_order_by(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collection_with_distinct_projecting_identifier_column(bool async) =>
        base.Correlated_collection_with_distinct_projecting_identifier_column(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collection_with_distinct_not_projecting_identifier_column(bool async) =>
        base.Correlated_collection_with_distinct_not_projecting_identifier_column(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Correlated_collection_with_groupby_not_projecting_identifier_column_but_only_grouping_key_in_final_projection(
            bool async
        ) =>
        base
            .Correlated_collection_with_groupby_not_projecting_identifier_column_but_only_grouping_key_in_final_projection(
                async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Correlated_collection_with_groupby_not_projecting_identifier_column_with_group_aggregate_in_final_projection(
            bool async
        ) =>
        base
            .Correlated_collection_with_groupby_not_projecting_identifier_column_with_group_aggregate_in_final_projection(
                async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Correlated_collection_with_groupby_not_projecting_identifier_column_with_group_aggregate_in_final_projection_multiple_grouping_keys(
            bool async
        ) =>
        base
            .Correlated_collection_with_groupby_not_projecting_identifier_column_with_group_aggregate_in_final_projection_multiple_grouping_keys(
                async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Correlated_collection_with_groupby_with_complex_grouping_key_not_projecting_identifier_column_with_group_aggregate_in_final_projection(
            bool async
        ) =>
        base
            .Correlated_collection_with_groupby_with_complex_grouping_key_not_projecting_identifier_column_with_group_aggregate_in_final_projection(
                async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Correlated_collection_via_SelectMany_with_Distinct_missing_indentifying_columns_in_projection(bool async) =>
        base.Correlated_collection_via_SelectMany_with_Distinct_missing_indentifying_columns_in_projection(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collection_after_distinct_3_levels(bool async) =>
        base.Correlated_collection_after_distinct_3_levels(async);

#if EFCORE9
    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_DateOnly_Month(bool async) => base.Where_DateOnly_Month(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_DateOnly_AddYears(bool async) => base.Where_DateOnly_AddYears(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_DateOnly_AddMonths(bool async) => base.Where_DateOnly_AddMonths(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_DateOnly_AddDays(bool async) => base.Where_DateOnly_AddDays(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_TimeOnly_Hour(bool async) => base.Where_TimeOnly_Hour(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_TimeOnly_Minute(bool async) => base.Where_TimeOnly_Minute(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_TimeOnly_Second(bool async) => base.Where_TimeOnly_Second(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_TimeOnly_Millisecond(bool async) => base.Where_TimeOnly_Millisecond(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_TimeOnly_AddHours(bool async) => base.Where_TimeOnly_AddHours(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_TimeOnly_AddMinutes(bool async) => base.Where_TimeOnly_AddMinutes(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_TimeOnly_Add_TimeSpan(bool async) => base.Where_TimeOnly_Add_TimeSpan(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_TimeOnly_IsBetween(bool async) => base.Where_TimeOnly_IsBetween(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_TimeOnly_subtract_TimeOnly(bool async) => base.Where_TimeOnly_subtract_TimeOnly(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_TimeOnly_FromDateTime_compared_to_property(bool async) =>
        base.Where_TimeOnly_FromDateTime_compared_to_property(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_TimeOnly_FromDateTime_compared_to_parameter(bool async) =>
        base.Where_TimeOnly_FromDateTime_compared_to_parameter(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_TimeOnly_FromDateTime_compared_to_constant(bool async) =>
        base.Where_TimeOnly_FromDateTime_compared_to_constant(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_TimeOnly_FromTimeSpan_compared_to_property(bool async) =>
        base.Where_TimeOnly_FromTimeSpan_compared_to_property(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_TimeOnly_FromTimeSpan_compared_to_parameter(bool async) =>
        base.Where_TimeOnly_FromTimeSpan_compared_to_parameter(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Order_by_TimeOnly_FromTimeSpan(bool async) => base.Order_by_TimeOnly_FromTimeSpan(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_DateOnly_FromDateTime_compared_to_property(bool async) =>
        base.Where_DateOnly_FromDateTime_compared_to_property(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_DateOnly_FromDateTime_compared_to_constant_and_parameter(bool async) =>
        base.Where_DateOnly_FromDateTime_compared_to_constant_and_parameter(async);
#endif


    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Project_navigation_defined_on_base_from_entity_with_inheritance_using_soft_cast(bool async) =>
        base.Project_navigation_defined_on_base_from_entity_with_inheritance_using_soft_cast(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Project_navigation_defined_on_derived_from_entity_with_inheritance_using_soft_cast(bool async) =>
        base.Project_navigation_defined_on_derived_from_entity_with_inheritance_using_soft_cast(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Join_entity_with_itself_grouped_by_key_followed_by_include_skip_take(bool async) =>
        base.Join_entity_with_itself_grouped_by_key_followed_by_include_skip_take(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Enum_matching_take_value_gets_different_type_mapping(bool async) =>
        base.Enum_matching_take_value_gets_different_type_mapping(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_equality_to_null_with_composite_key(bool async) =>
        base.Where_subquery_equality_to_null_with_composite_key(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_equality_to_null_with_composite_key_should_match_nulls(bool async) =>
        base.Where_subquery_equality_to_null_with_composite_key_should_match_nulls(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_equality_to_null_without_composite_key(bool async) =>
        base.Where_subquery_equality_to_null_without_composite_key(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_equality_to_null_without_composite_key_should_match_null(bool async) =>
        base.Where_subquery_equality_to_null_without_composite_key_should_match_null(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_with_ElementAtOrDefault_equality_to_null_with_composite_key(bool async) =>
        base.Where_subquery_with_ElementAtOrDefault_equality_to_null_with_composite_key(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_with_ElementAt_using_column_as_index(bool async) =>
        base.Where_subquery_with_ElementAt_using_column_as_index(async);

#if EFCORE9
    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DateTimeOffset_to_unix_time_milliseconds(bool async) =>
        base.DateTimeOffset_to_unix_time_milliseconds(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DateTimeOffset_to_unix_time_seconds(bool async) =>
        base.DateTimeOffset_to_unix_time_seconds(async);
#endif

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Set_operator_with_navigation_in_projection_groupby_aggregate(bool async) =>
        base.Set_operator_with_navigation_in_projection_groupby_aggregate(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Nav_expansion_inside_Contains_argument(bool async) =>
        base.Nav_expansion_inside_Contains_argument(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Nav_expansion_with_member_pushdown_inside_Contains_argument(bool async) =>
        base.Nav_expansion_with_member_pushdown_inside_Contains_argument(async);

    [ConditionalTheory(Skip = "TODO: Fix tests")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_inside_Take_argument(bool async) => base.Subquery_inside_Take_argument(async);
}
