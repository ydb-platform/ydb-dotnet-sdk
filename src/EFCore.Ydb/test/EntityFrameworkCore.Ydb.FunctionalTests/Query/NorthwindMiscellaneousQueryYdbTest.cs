using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.Northwind;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class NorthwindMiscellaneousQueryYdbTest(NorthwindQueryYdbFixture<NoopModelCustomizer> fixture)
    : NorthwindMiscellaneousQueryRelationalTestBase<NorthwindQueryYdbFixture<NoopModelCustomizer>>(fixture)
{
    public override async Task Client_code_using_instance_method_throws(bool async) =>
        Assert.Equal(
            CoreStrings.ClientProjectionCapturingConstantInMethodInstance(
                typeof(NorthwindMiscellaneousQueryYdbTest).FullName, "InstanceMethod"),
            (await Assert.ThrowsAsync<InvalidOperationException>(() =>
                base.Client_code_using_instance_method_throws(async))).Message);

    public override async Task Client_code_using_instance_in_static_method(bool async) =>
        Assert.Equal(
            CoreStrings.ClientProjectionCapturingConstantInMethodArgument(
                typeof(NorthwindMiscellaneousQueryYdbTest).FullName, "StaticMethod"),
            (await Assert.ThrowsAsync<InvalidOperationException>(() =>
                base.Client_code_using_instance_in_static_method(async))).Message);

    public override async Task Client_code_using_instance_in_anonymous_type(bool async) =>
        Assert.Equal(
            CoreStrings.ClientProjectionCapturingConstantInTree(typeof(NorthwindMiscellaneousQueryYdbTest).FullName),
            (await Assert.ThrowsAsync<InvalidOperationException>(() =>
                base.Client_code_using_instance_in_anonymous_type(async))).Message);

    public override Task Client_code_unknown_method(bool async)
        => AssertTranslationFailed(() => base.Client_code_unknown_method(async));

    public override async Task Entity_equality_through_subquery_composite_key(bool async) =>
        Assert.Equal(
            CoreStrings.EntityEqualityOnCompositeKeyEntitySubqueryNotSupported("==", nameof(OrderDetail)),
            (await Assert.ThrowsAsync<InvalidOperationException>(() =>
                base.Entity_equality_through_subquery_composite_key(async))).Message);

    [ConditionalTheory(Skip = "DateTime subtraction is not currently translated to a TimeSpan result.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Add_minutes_on_constant_value(bool async) => base.Add_minutes_on_constant_value(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task All_on_distinct(bool async) => base.All_on_distinct(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task All_top_level_subquery(bool async) => base.All_top_level_subquery(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task All_top_level_subquery_ef_property(bool async)
        => base.All_top_level_subquery_ef_property(async);

    [ConditionalTheory(Skip = "YDB rejects ORDER BY constant expressions generated for this query.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Anonymous_subquery_orderby(bool async) => base.Anonymous_subquery_orderby(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Any_on_distinct(bool async) => base.Any_on_distinct(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Any_with_multiple_conditions_still_uses_exists(bool async)
        => base.Any_with_multiple_conditions_still_uses_exists(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Collection_navigation_equal_to_null_for_subquery(bool async)
        => base.Collection_navigation_equal_to_null_for_subquery(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Collection_navigation_equal_to_null_for_subquery_using_ElementAtOrDefault_constant_one(
        bool async)
        => base.Collection_navigation_equal_to_null_for_subquery_using_ElementAtOrDefault_constant_one(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Collection_navigation_equal_to_null_for_subquery_using_ElementAtOrDefault_constant_zero(bool async)
        => base.Collection_navigation_equal_to_null_for_subquery_using_ElementAtOrDefault_constant_zero(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Collection_navigation_equal_to_null_for_subquery_using_ElementAtOrDefault_parameter(bool async)
        => base.Collection_navigation_equal_to_null_for_subquery_using_ElementAtOrDefault_parameter(async);

    [ConditionalTheory(Skip = "YDB JOIN ON requires conjunctions of equality predicates.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Collection_projection_after_DefaultIfEmpty(bool async)
        => base.Collection_projection_after_DefaultIfEmpty(async);

    [ConditionalTheory(Skip = "YDB does not support APPLY-based correlated query translations.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Complex_nested_query_doesnt_try_binding_to_grandparent_when_parent_returns_complex_result(bool async)
        => base.Complex_nested_query_doesnt_try_binding_to_grandparent_when_parent_returns_complex_result(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Complex_nested_query_properly_binds_to_grandparent_when_parent_returns_scalar_result(
        bool async)
        => base.Complex_nested_query_properly_binds_to_grandparent_when_parent_returns_scalar_result(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Contains_on_distinct(bool async) => base.Contains_on_distinct(async);

    [ConditionalTheory(Skip = "YDB does not support APPLY-based correlated query translations.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Correlated_collection_with_distinct_without_default_identifiers_projecting_columns(bool async)
        => base.Correlated_collection_with_distinct_without_default_identifiers_projecting_columns(async);

    [ConditionalTheory(Skip = "YDB does not support APPLY-based correlated query translations.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Correlated_collection_with_distinct_without_default_identifiers_projecting_columns_with_navigation(bool async)
        => base.Correlated_collection_with_distinct_without_default_identifiers_projecting_columns_with_navigation(
            async);

    [ConditionalTheory(Skip = "YDB rejects ORDER BY constant expressions generated for this query.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DTO_subquery_orderby(bool async) => base.DTO_subquery_orderby(async);

    [ConditionalTheory(Skip = "YDB JOIN ON requires conjunctions of equality predicates.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DefaultIfEmpty_Sum_over_collection_navigation(bool async)
        => base.DefaultIfEmpty_Sum_over_collection_navigation(async);

    [ConditionalTheory(Skip = "YDB JOIN ON requires conjunctions of equality predicates.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DefaultIfEmpty_in_subquery_nested(bool async)
        => base.DefaultIfEmpty_in_subquery_nested(async);

    [ConditionalTheory(Skip = "Nested correlated DefaultIfEmpty cannot currently be translated.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DefaultIfEmpty_in_subquery_nested_filter_order_comparison(bool async)
        => base.DefaultIfEmpty_in_subquery_nested_filter_order_comparison(async);

    [ConditionalTheory(Skip = "YDB JOIN ON requires conjunctions of equality predicates.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DefaultIfEmpty_in_subquery_not_correlated(bool async)
        => base.DefaultIfEmpty_in_subquery_not_correlated(async);

    [ConditionalTheory(Skip = "YDB JOIN ON requires conjunctions of equality predicates.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DefaultIfEmpty_over_empty_collection_followed_by_projecting_constant(bool async)
        => base.DefaultIfEmpty_over_empty_collection_followed_by_projecting_constant(async);

    [ConditionalTheory(Skip = "YDB JOIN ON requires conjunctions of equality predicates.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DefaultIfEmpty_without_group_join(bool async)
        => base.DefaultIfEmpty_without_group_join(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Dependent_to_principal_navigation_equal_to_null_for_subquery(bool async)
        => base.Dependent_to_principal_navigation_equal_to_null_for_subquery(async);

    [ConditionalTheory(Skip = "Ordering a Distinct subquery by this conditional predicate cannot be translated.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Distinct_followed_by_ordering_on_condition(bool async)
        => base.Distinct_followed_by_ordering_on_condition(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Entity_equality_on_subquery_with_null_check(bool async)
        => base.Entity_equality_on_subquery_with_null_check(async);

    [ConditionalTheory(Skip = "YDB rejects ORDER BY constant expressions generated for this query.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Entity_equality_orderby_descending_subquery_composite_key(bool async)
        => base.Entity_equality_orderby_descending_subquery_composite_key(async);

    [ConditionalTheory(Skip = "YDB rejects ORDER BY constant expressions generated for this query.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Entity_equality_orderby_subquery(bool async)
        => base.Entity_equality_orderby_subquery(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Entity_equality_through_subquery(bool async)
        => base.Entity_equality_through_subquery(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task FirstOrDefault_with_predicate_nested(bool async)
        => base.FirstOrDefault_with_predicate_nested(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task First_on_collection_in_projection(bool async)
        => base.First_on_collection_in_projection(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Inner_parameter_in_nested_lambdas_gets_preserved(bool async)
        => base.Inner_parameter_in_nested_lambdas_gets_preserved(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Let_any_subquery_anonymous(bool async) => base.Let_any_subquery_anonymous(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Let_entity_equality_to_null(bool async) => base.Let_entity_equality_to_null(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Let_entity_equality_to_other_entity(bool async)
        => base.Let_entity_equality_to_other_entity(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Let_subquery_with_multiple_occurrences(bool async)
        => base.Let_subquery_with_multiple_occurrences(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Max_on_empty_sequence_throws(bool async) => base.Max_on_empty_sequence_throws(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task OrderBy_any(bool async) => base.OrderBy_any(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task OrderBy_correlated_subquery1(bool async) => base.OrderBy_correlated_subquery1(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Pending_selector_in_cardinality_reducing_method_is_applied_before_expanding_collection_navigation_member(
            bool async)
        => base
            .Pending_selector_in_cardinality_reducing_method_is_applied_before_expanding_collection_navigation_member(
                async);

    [ConditionalTheory(Skip = "The generated query contains duplicate column names rejected by YDB.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Projecting_collection_split(bool async) => base.Projecting_collection_split(async);

    [ConditionalTheory(Skip = "The generated query contains duplicate column names rejected by YDB.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Projecting_collection_then_include_split(bool async)
        => base.Projecting_collection_then_include_split(async);

    [ConditionalTheory(Skip = "Collection projections with Skip or Take do not preserve the expected cardinality.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Projection_skip_collection_projection(bool async)
        => base.Projection_skip_collection_projection(async);

    [ConditionalTheory(Skip = "Collection projections with Skip or Take do not preserve the expected cardinality.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Projection_skip_take_collection_projection(bool async)
        => base.Projection_skip_take_collection_projection(async);

    [ConditionalTheory(Skip = "Collection projections with Skip or Take do not preserve the expected cardinality.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Projection_take_collection_projection(bool async)
        => base.Projection_take_collection_projection(async);

    [ConditionalTheory(Skip = "Numeric ToString in this composed predicate is not translated.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Query_expression_with_to_string_and_contains(bool async)
        => base.Query_expression_with_to_string_and_contains(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Query_when_evaluatable_queryable_method_call_with_repository(bool async)
        => base.Query_when_evaluatable_queryable_method_call_with_repository(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task SelectMany_Joined_DefaultIfEmpty3(bool async)
        => base.SelectMany_Joined_DefaultIfEmpty3(async);

    [ConditionalTheory(Skip = "A UInt64 aggregate result cannot currently be read as nullable Int64.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task SelectMany_LongCount(bool async) => base.SelectMany_LongCount(async);

    [ConditionalTheory(Skip = "YDB does not support APPLY-based correlated query translations.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task SelectMany_correlated_subquery_hard(bool async)
        => base.SelectMany_correlated_subquery_hard(async);

    [ConditionalTheory(Skip = "Distinct DTO collection projections hit upstream relational issue #24478.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task
        Select_DTO_constructor_distinct_with_collection_projection_translated_to_server_with_binding_after_client_eval(
            bool async)
        => base
            .Select_DTO_constructor_distinct_with_collection_projection_translated_to_server_with_binding_after_client_eval(
                async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_Where_Subquery_Deep_First(bool async)
        => base.Select_Where_Subquery_Deep_First(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_Where_Subquery_Deep_Single(bool async)
        => base.Select_Where_Subquery_Deep_Single(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_Where_Subquery_Equality(bool async) => base.Select_Where_Subquery_Equality(async);

    [ConditionalTheory(Skip = "YDB does not support APPLY-based correlated query translations.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_correlated_subquery_ordered(bool async)
        => base.Select_correlated_subquery_ordered(async);

    [ConditionalTheory(Skip = "A UInt64 aggregate result cannot currently be read as nullable Int64.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_distinct_long_count(bool async) => base.Select_distinct_long_count(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_nested_collection_count_using_DTO(bool async)
        => base.Select_nested_collection_count_using_DTO(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_nested_collection_with_distinct(bool async)
        => base.Select_nested_collection_with_distinct(async);

    [ConditionalTheory(Skip = "A UInt64 aggregate result cannot currently be read as nullable Int64.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_orderBy_skip_long_count(bool async) => base.Select_orderBy_skip_long_count(async);

    [ConditionalTheory(Skip = "A UInt64 aggregate result cannot currently be read as nullable Int64.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_orderBy_take_long_count(bool async) => base.Select_orderBy_take_long_count(async);

    [ConditionalTheory(Skip = "A UInt64 aggregate result cannot currently be read as nullable Int64.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_skip_long_count(bool async) => base.Select_skip_long_count(async);

    [ConditionalTheory(Skip = "YDB does not support APPLY-based correlated query translations.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_subquery_recursive_trivial(bool async)
        => base.Select_subquery_recursive_trivial(async);

    [ConditionalTheory(Skip = "A UInt64 aggregate result cannot currently be read as nullable Int64.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Select_take_long_count(bool async) => base.Select_take_long_count(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Skip_0_Take_0_works_when_constant(bool async)
        => base.Skip_0_Take_0_works_when_constant(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Skip_1_Take_0_works_when_constant(bool async)
        => base.Skip_1_Take_0_works_when_constant(async);

    [ConditionalTheory(Skip = "YDB rejects ORDER BY constant expressions generated for this query.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Skip_orderby_const(bool async) => base.Skip_orderby_const(async);

    [ConditionalTheory(Skip = "YDB JOIN ON requires conjunctions of equality predicates.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_DefaultIfEmpty_Any(bool async) => base.Subquery_DefaultIfEmpty_Any(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_is_not_null_translated_correctly(bool async)
        => base.Subquery_is_not_null_translated_correctly(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_is_null_translated_correctly(bool async)
        => base.Subquery_is_null_translated_correctly(async);

    [ConditionalTheory(Skip = "YDB rejects ORDER BY constant expressions generated for this query.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_member_pushdown_does_not_change_original_subquery_model(bool async)
        => base.Subquery_member_pushdown_does_not_change_original_subquery_model(async);

    [ConditionalTheory(Skip = "YDB rejects ORDER BY constant expressions generated for this query.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_member_pushdown_does_not_change_original_subquery_model2(bool async)
        => base.Subquery_member_pushdown_does_not_change_original_subquery_model2(async);

    [ConditionalTheory(Skip = "YDB rejects the generated query: mismatched input 'AS' expecting {<EOF>, ';'}")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Subquery_with_navigation_inside_inline_collection(bool async)
        => base.Subquery_with_navigation_inside_inline_collection(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Take_0_works_when_constant(bool async) => base.Take_0_works_when_constant(async);

    [ConditionalFact(Skip = "Cancellation does not currently emit the expected QueryCanceled event.")]
    public override Task ToListAsync_can_be_canceled() => base.ToListAsync_can_be_canceled();

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_Join_Any(bool async) => base.Where_Join_Any(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_Order_First(bool async) => base.Where_Order_First(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_query_composition_entity_equality_multiple_elements_First(bool async)
        => base.Where_query_composition_entity_equality_multiple_elements_First(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_query_composition_entity_equality_multiple_elements_FirstOrDefault(bool async)
        => base.Where_query_composition_entity_equality_multiple_elements_FirstOrDefault(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_query_composition_entity_equality_multiple_elements_Single(bool async)
        => base.Where_query_composition_entity_equality_multiple_elements_Single(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_query_composition_entity_equality_multiple_elements_SingleOrDefault(bool async)
        => base.Where_query_composition_entity_equality_multiple_elements_SingleOrDefault(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_query_composition_entity_equality_one_element_First(bool async)
        => base.Where_query_composition_entity_equality_one_element_First(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_query_composition_entity_equality_one_element_FirstOrDefault(bool async)
        => base.Where_query_composition_entity_equality_one_element_FirstOrDefault(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_query_composition_entity_equality_one_element_Single(bool async)
        => base.Where_query_composition_entity_equality_one_element_Single(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_query_composition_entity_equality_one_element_SingleOrDefault(bool async)
        => base.Where_query_composition_entity_equality_one_element_SingleOrDefault(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_query_composition_is_not_null(bool async)
        => base.Where_query_composition_is_not_null(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_query_composition_is_null(bool async) => base.Where_query_composition_is_null(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_expression_same_parametername(bool async)
        => base.Where_subquery_expression_same_parametername(async);

    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_subquery_on_collection(bool async) => base.Where_subquery_on_collection(async);

#if EFCORE9
    [ConditionalTheory(Skip = "The generated query references a member outside its YDB row scope.")] // #35118
    [MemberData(nameof(IsAsyncData))]
    public override Task Column_access_inside_subquery_predicate(bool async)
        => base.Column_access_inside_subquery_predicate(async);

    [ConditionalTheory(Skip = "YDB JOIN ON requires conjunctions of equality predicates.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Default_if_empty_top_level(bool async) => base.Default_if_empty_top_level(async);

    [ConditionalTheory(Skip = "YDB JOIN ON requires conjunctions of equality predicates.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Default_if_empty_top_level_followed_by_projecting_constant(bool async)
        => base.Default_if_empty_top_level_followed_by_projecting_constant(async);

    [ConditionalTheory(Skip = "YDB JOIN ON requires conjunctions of equality predicates.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Default_if_empty_top_level_positive(bool async)
        => base.Default_if_empty_top_level_positive(async);

    [ConditionalTheory(Skip = "YDB JOIN ON requires conjunctions of equality predicates.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Default_if_empty_top_level_projection(bool async)
        => base.Default_if_empty_top_level_projection(async);

    [ConditionalTheory(Skip = "YDB JOIN ON requires conjunctions of equality predicates.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Join_with_default_if_empty_on_both_sources(bool async)
        => base.Join_with_default_if_empty_on_both_sources(async);

    [ConditionalTheory(Skip = "YDB rejects ORDER BY constant expressions generated for this query.")] // #32234
    [MemberData(nameof(IsAsyncData))]
    public override Task Parameter_collection_Contains_with_projection_and_ordering(bool async)
        => base.Parameter_collection_Contains_with_projection_and_ordering(async);

    [ConditionalTheory(Skip = "YDB bitwise operations require unsigned operands; Northwind uses Int32 storage.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_bitwise_binary_and(bool async) => base.Where_bitwise_binary_and(async);

    [ConditionalTheory(Skip = "YDB bitwise operations require unsigned operands; Northwind uses Int32 storage.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_bitwise_binary_not(bool async) => base.Where_bitwise_binary_not(async);

    [ConditionalTheory(Skip = "YDB bitwise operations require unsigned operands; Northwind uses Int32 storage.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_bitwise_binary_or(bool async) => base.Where_bitwise_binary_or(async);

    [ConditionalTheory(Skip = "YDB bitwise operations require unsigned operands; Northwind uses Int32 storage.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_bitwise_binary_xor(bool async) => base.Where_bitwise_binary_xor(async);
#else
    [ConditionalTheory(Skip =
        "The generated query references a member outside its YDB row scope.")]
    [MemberData(nameof(IsAsyncData))] // #35118
    public override Task Column_access_inside_subquery_predicate(bool async)
        => base.Column_access_inside_subquery_predicate(async);

    [ConditionalTheory(Skip =
        "Contains over DateTime.Date leaves the dates parameter without a type mapping.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Contains_with_DateTime_Date(bool async) => base.Contains_with_DateTime_Date(async);

    [ConditionalTheory(Skip =
        "YDB JOIN ON requires conjunctions of equality predicates.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DefaultIfEmpty_top_level(bool async) => base.DefaultIfEmpty_top_level(async);

    [ConditionalTheory(Skip =
        "YDB JOIN ON requires conjunctions of equality predicates.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DefaultIfEmpty_top_level_followed_by_constant_Select(bool async)
        => base.DefaultIfEmpty_top_level_followed_by_constant_Select(async);

    [ConditionalTheory(Skip =
        "YDB JOIN ON requires conjunctions of equality predicates.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DefaultIfEmpty_top_level_positive(bool async)
        => base.DefaultIfEmpty_top_level_positive(async);

    [ConditionalTheory(Skip =
        "YDB JOIN ON requires conjunctions of equality predicates.")]
    [MemberData(nameof(IsAsyncData))] // #36208
    public override Task DefaultIfEmpty_top_level_preceded_by_constant_Select(bool async)
        => base.DefaultIfEmpty_top_level_preceded_by_constant_Select(async);

    [ConditionalTheory(Skip =
        "YDB JOIN ON requires conjunctions of equality predicates.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task DefaultIfEmpty_top_level_projection(bool async)
        => base.DefaultIfEmpty_top_level_projection(async);

    [ConditionalTheory(Skip =
        "YDB JOIN ON requires conjunctions of equality predicates.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Join_with_DefaultIfEmpty_on_both_sources(bool async)
        => base.Join_with_DefaultIfEmpty_on_both_sources(async);

    [ConditionalTheory(Skip =
        "YDB rejects the generated query: Column o is not in source column set")]
    [MemberData(nameof(IsAsyncData))] // #35507
    public override Task Late_subquery_pushdown(bool async) => base.Late_subquery_pushdown(async);

    [ConditionalTheory(Skip =
        "YDB rejects ORDER BY constant expressions generated for this query.")]
    [MemberData(nameof(IsAsyncData))] // #32234
    public override Task Parameter_collection_Contains_with_projection_and_ordering(bool async)
        => base.Parameter_collection_Contains_with_projection_and_ordering(async);

    [ConditionalTheory(Skip =
        "YDB does not support APPLY-based correlated query translations.")]
    [MemberData(nameof(IsAsyncData))] // #35950
    public override Task SelectMany_correlated_with_Select_value_type_and_DefaultIfEmpty_in_selector(bool async)
        => base.SelectMany_correlated_with_Select_value_type_and_DefaultIfEmpty_in_selector(async);

    [ConditionalTheory(Skip =
        "DateTime Microsecond and Nanosecond predicates are not currently translated.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_nanosecond_and_microsecond_component(bool async)
        => base.Where_nanosecond_and_microsecond_component(async);
#endif
}
