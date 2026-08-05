using Microsoft.EntityFrameworkCore.BulkUpdates;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;
using Xunit.Abstractions;
using static EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities.SharedTestMethods;

namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

public class NorthwindBulkUpdatesYdbTest(
    NorthwindBulkUpdatesYdbFixture<NoopModelCustomizer> fixture,
    ITestOutputHelper testOutputHelper
) : NorthwindBulkUpdatesRelationalTestBase<NorthwindBulkUpdatesYdbFixture<NoopModelCustomizer>>(
    fixture,
    testOutputHelper
)
{
    private const string UnsupportedBulkOperationSkipReason = "YDB does not support this bulk operation shape";

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_FromSql_converted_to_subquery(bool async)
        => AssertBulkOperation(base.Delete_FromSql_converted_to_subquery, async);

    public override Task Delete_Where_TagWith(bool async)
        => AssertBulkOperation(base.Delete_Where_TagWith, async);

    public override Task Delete_Where(bool async)
        => AssertBulkOperation(base.Delete_Where, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_Where_parameter(bool async)
        => AssertBulkOperation(base.Delete_Where_parameter, async);

    public override Task Delete_Where_OrderBy(bool async)
        => AssertBulkOperation(base.Delete_Where_OrderBy, async);

    public override Task Delete_Where_OrderBy_Skip(bool async)
        => AssertBulkOperation(base.Delete_Where_OrderBy_Skip, async);

    public override Task Delete_Where_OrderBy_Take(bool async)
        => AssertBulkOperation(base.Delete_Where_OrderBy_Take, async);

    public override Task Delete_Where_OrderBy_Skip_Take(bool async)
        => AssertBulkOperation(base.Delete_Where_OrderBy_Skip_Take, async);

    public override Task Delete_Where_Skip(bool async)
        => AssertBulkOperation(base.Delete_Where_Skip, async);

    public override Task Delete_Where_Take(bool async)
        => AssertBulkOperation(base.Delete_Where_Take, async);

    public override Task Delete_Where_Skip_Take(bool async)
        => AssertBulkOperation(base.Delete_Where_Skip_Take, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_Where_predicate_with_GroupBy_aggregate(bool async)
        => AssertBulkOperation(base.Delete_Where_predicate_with_GroupBy_aggregate, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_Where_predicate_with_GroupBy_aggregate_2(bool async)
        => AssertBulkOperation(base.Delete_Where_predicate_with_GroupBy_aggregate_2, async);

    public override Task Delete_GroupBy_Where_Select(bool async)
        => AssertBulkOperation(base.Delete_GroupBy_Where_Select, async);

    public override Task Delete_GroupBy_Where_Select_2(bool async)
        => AssertBulkOperation(base.Delete_GroupBy_Where_Select_2, async);

    public override Task Delete_Where_Skip_Take_Skip_Take_causing_subquery(bool async)
        => AssertBulkOperation(base.Delete_Where_Skip_Take_Skip_Take_causing_subquery, async);

    public override Task Delete_Where_Distinct(bool async)
        => AssertBulkOperation(base.Delete_Where_Distinct, async);

    public override Task Delete_SelectMany(bool async)
        => AssertBulkOperation(base.Delete_SelectMany, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_SelectMany_subquery(bool async)
        => AssertBulkOperation(base.Delete_SelectMany_subquery, async);

    public override Task Delete_Where_using_navigation(bool async)
        => AssertBulkOperation(base.Delete_Where_using_navigation, async);

    public override Task Delete_Where_using_navigation_2(bool async)
        => AssertBulkOperation(base.Delete_Where_using_navigation_2, async);

    public override Task Delete_Union(bool async)
        => AssertBulkOperation(base.Delete_Union, async);

    public override Task Delete_Concat(bool async)
        => AssertBulkOperation(base.Delete_Concat, async);

    public override Task Delete_Intersect(bool async)
        => AssertBulkOperation(base.Delete_Intersect, async);

    public override Task Delete_Except(bool async)
        => AssertBulkOperation(base.Delete_Except, async);

    public override Task Delete_Where_optional_navigation_predicate(bool async)
        => AssertBulkOperation(base.Delete_Where_optional_navigation_predicate, async);

    public override Task Delete_with_join(bool async)
        => AssertBulkOperation(base.Delete_with_join, async);

#if EFCORE9
    public override Task Delete_with_left_join(bool async)
        => AssertBulkOperation(base.Delete_with_left_join, async);
#else
    public override Task Delete_with_LeftJoin(bool async)
        => AssertBulkOperation(base.Delete_with_LeftJoin, async);

    public override Task Delete_with_LeftJoin_via_flattened_GroupJoin(bool async)
        => AssertBulkOperation(base.Delete_with_LeftJoin_via_flattened_GroupJoin, async);
#endif

    public override Task Delete_with_cross_join(bool async)
        => AssertBulkOperation(base.Delete_with_cross_join, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_with_cross_apply(bool async)
        => AssertBulkOperation(base.Delete_with_cross_apply, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_with_outer_apply(bool async)
        => AssertBulkOperation(base.Delete_with_outer_apply, async);

#if !EFCORE9
    public override Task Delete_with_RightJoin(bool async)
        => AssertBulkOperation(base.Delete_with_RightJoin, async);
#endif

    public override Task Update_without_property_to_set_throws(bool async) =>
        base.Update_without_property_to_set_throws(async);

#if EFCORE9
    public override Task Update_with_invalid_lambda_throws(bool async) =>
        base.Update_with_invalid_lambda_throws(async);
#endif

    public override Task Update_with_invalid_lambda_in_set_property_throws(bool async) =>
        base.Update_with_invalid_lambda_in_set_property_throws(async);

    public override Task Update_FromSql_set_constant(bool async)
        => AssertBulkOperation(base.Update_FromSql_set_constant, async);

    public override Task Update_Where_set_constant_TagWith(bool async)
        => AssertBulkOperation(base.Update_Where_set_constant_TagWith, async);

    public override Task Update_Where_set_constant(bool async)
        => AssertBulkOperation(base.Update_Where_set_constant, async);

#if !EFCORE9
    public override Task Update_Where_set_constant_via_lambda(bool async)
        => AssertBulkOperation(base.Update_Where_set_constant_via_lambda, async);
#endif

    public override Task Update_Where_parameter_set_constant(bool async)
        => AssertBulkOperation(base.Update_Where_parameter_set_constant, async);

    public override Task Update_Where_set_parameter(bool async)
        => AssertBulkOperation(base.Update_Where_set_parameter, async);

    public override Task Update_Where_set_parameter_from_closure_array(bool async)
        => AssertBulkOperation(base.Update_Where_set_parameter_from_closure_array, async);

    public override Task Update_Where_set_parameter_from_inline_list(bool async)
        => AssertBulkOperation(base.Update_Where_set_parameter_from_inline_list, async);

    public override Task Update_Where_set_parameter_from_multilevel_property_access(bool async)
        => AssertBulkOperation(base.Update_Where_set_parameter_from_multilevel_property_access, async);

    public override Task Update_Where_Skip_set_constant(bool async)
        => AssertBulkOperation(base.Update_Where_Skip_set_constant, async);

    public override Task Update_Where_Take_set_constant(bool async)
        => AssertBulkOperation(base.Update_Where_Take_set_constant, async);

    public override Task Update_Where_Skip_Take_set_constant(bool async)
        => AssertBulkOperation(base.Update_Where_Skip_Take_set_constant, async);

    public override Task Update_Where_OrderBy_set_constant(bool async)
        => AssertBulkOperation(base.Update_Where_OrderBy_set_constant, async);

    public override Task Update_Where_OrderBy_Skip_set_constant(bool async)
        => AssertBulkOperation(base.Update_Where_OrderBy_Skip_set_constant, async);

    public override Task Update_Where_OrderBy_Take_set_constant(bool async)
        => AssertBulkOperation(base.Update_Where_OrderBy_Take_set_constant, async);

    public override Task Update_Where_OrderBy_Skip_Take_set_constant(bool async)
        => AssertBulkOperation(base.Update_Where_OrderBy_Skip_Take_set_constant, async);

    public override Task Update_Where_OrderBy_Skip_Take_Skip_Take_set_constant(bool async)
        => AssertBulkOperation(base.Update_Where_OrderBy_Skip_Take_Skip_Take_set_constant, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_GroupBy_aggregate_set_constant(bool async)
        => AssertBulkOperation(base.Update_Where_GroupBy_aggregate_set_constant, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_GroupBy_First_set_constant(bool async)
        => AssertBulkOperation(base.Update_Where_GroupBy_First_set_constant, async);

    public override Task Update_Where_GroupBy_First_set_constant_2(bool async)
        => AssertBulkOperation(base.Update_Where_GroupBy_First_set_constant_2, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_GroupBy_First_set_constant_3(bool async)
        => AssertBulkOperation(base.Update_Where_GroupBy_First_set_constant_3, async);

    public override Task Update_Where_Distinct_set_constant(bool async)
        => AssertBulkOperation(base.Update_Where_Distinct_set_constant, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_using_navigation_set_null(bool async)
        => AssertBulkOperation(base.Update_Where_using_navigation_set_null, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_using_navigation_2_set_constant(bool async)
        => AssertBulkOperation(base.Update_Where_using_navigation_2_set_constant, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_SelectMany_set_null(bool async)
        => AssertBulkOperation(base.Update_Where_SelectMany_set_null, async);

    public override Task Update_Where_set_property_plus_constant(bool async)
        => AssertBulkOperation(base.Update_Where_set_property_plus_constant, async);

    public override Task Update_Where_set_property_plus_parameter(bool async)
        => AssertBulkOperation(base.Update_Where_set_property_plus_parameter, async);

    public override Task Update_Where_set_property_plus_property(bool async)
        => AssertBulkOperation(base.Update_Where_set_property_plus_property, async);

    public override Task Update_Where_set_constant_using_ef_property(bool async)
        => AssertBulkOperation(base.Update_Where_set_constant_using_ef_property, async);

    public override Task Update_Where_set_null(bool async)
        => AssertBulkOperation(base.Update_Where_set_null, async);

    public override Task Update_Where_multiple_set(bool async)
        => AssertBulkOperation(base.Update_Where_multiple_set, async);

    public override Task Update_Union_set_constant(bool async)
        => AssertBulkOperation(base.Update_Union_set_constant, async);

    public override Task Update_Concat_set_constant(bool async)
        => AssertBulkOperation(base.Update_Concat_set_constant, async);

    public override Task Update_Except_set_constant(bool async)
        => AssertBulkOperation(base.Update_Except_set_constant, async);

    public override Task Update_Intersect_set_constant(bool async)
        => AssertBulkOperation(base.Update_Intersect_set_constant, async);

    public override Task Update_with_join_set_constant(bool async)
        => AssertBulkOperation(base.Update_with_join_set_constant, async);

#if EFCORE9
    public override Task Update_with_left_join_set_constant(bool async)
        => AssertBulkOperation(base.Update_with_left_join_set_constant, async);
#else
    public override Task Update_with_LeftJoin(bool async)
        => AssertBulkOperation(base.Update_with_LeftJoin, async);

    public override Task Update_with_LeftJoin_via_flattened_GroupJoin(bool async)
        => AssertBulkOperation(base.Update_with_LeftJoin_via_flattened_GroupJoin, async);

    public override Task Update_with_RightJoin(bool async)
        => AssertBulkOperation(base.Update_with_RightJoin, async);
#endif

    public override Task Update_with_cross_join_set_constant(bool async)
        => AssertBulkOperation(base.Update_with_cross_join_set_constant, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_with_cross_apply_set_constant(bool async)
        => AssertBulkOperation(base.Update_with_cross_apply_set_constant, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_with_outer_apply_set_constant(bool async)
        => AssertBulkOperation(base.Update_with_outer_apply_set_constant, async);

    public override Task Update_with_cross_join_left_join_set_constant(bool async)
        => AssertBulkOperation(base.Update_with_cross_join_left_join_set_constant, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_with_cross_join_cross_apply_set_constant(bool async)
        => AssertBulkOperation(base.Update_with_cross_join_cross_apply_set_constant, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_with_cross_join_outer_apply_set_constant(bool async)
        => AssertBulkOperation(base.Update_with_cross_join_outer_apply_set_constant, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_SelectMany_subquery_set_null(bool async)
        => AssertBulkOperation(base.Update_Where_SelectMany_subquery_set_null, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_Join_set_property_from_joined_single_result_table(bool async)
        => AssertBulkOperation(base.Update_Where_Join_set_property_from_joined_single_result_table, async);

    public override Task Update_Where_Join_set_property_from_joined_table(bool async)
        => AssertBulkOperation(base.Update_Where_Join_set_property_from_joined_table, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_Where_Join_set_property_from_joined_single_result_scalar(bool async)
        => AssertBulkOperation(base.Update_Where_Join_set_property_from_joined_single_result_scalar, async);

    [ConditionalTheory(Skip = UnsupportedBulkOperationSkipReason)]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_with_two_inner_joins(bool async)
        => AssertBulkOperation(base.Update_with_two_inner_joins, async);

    private Task AssertBulkOperation(Func<bool, Task> test, bool async)
        => AssertYdb(test, Fixture.TestSqlLoggerFactory, async);

    protected override void ClearLog()
        => Fixture.TestSqlLoggerFactory.Clear();
}
