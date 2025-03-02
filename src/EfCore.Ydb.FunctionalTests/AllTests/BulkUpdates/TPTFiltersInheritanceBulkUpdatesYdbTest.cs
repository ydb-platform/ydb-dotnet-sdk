using System.Text;
using Microsoft.EntityFrameworkCore.BulkUpdates;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace EfCore.Ydb.FunctionalTests.AllTests.BulkUpdates;

public class TPTFiltersInheritanceBulkUpdatesSqlServerTest(
    TPTFiltersInheritanceBulkUpdatesYdbFixture fixture,
    ITestOutputHelper testOutputHelper
) : TPTFiltersInheritanceBulkUpdatesTestBase<TPTFiltersInheritanceBulkUpdatesYdbFixture>(fixture, testOutputHelper)
{
    public override Task Delete_where_keyless_entity_mapped_to_sql_query(bool async)
        => TestIgnoringBase(
            base.Delete_where_keyless_entity_mapped_to_sql_query,
            async
        );

    public override Task Delete_where_hierarchy(bool async)
        => TestIgnoringBase(
            base.Delete_where_hierarchy,
            async
        );

    public override Task Delete_where_hierarchy_subquery(bool async)
        => TestIgnoringBase(
            base.Delete_where_hierarchy_subquery,
            async
        );

    public override Task Delete_where_hierarchy_derived(bool async)
        => TestIgnoringBase(
            base.Delete_where_hierarchy_derived,
            async
        );

    public override Task Delete_GroupBy_Where_Select_First_3(bool async)
        => TestIgnoringBase(
            base.Delete_GroupBy_Where_Select_First_3,
            async
        );

    public override Task Update_base_and_derived_types(bool async)
        => TestIgnoringBase(
            base.Update_base_and_derived_types,
            async
        );

    public override Task Update_where_keyless_entity_mapped_to_sql_query(bool async)
        => TestIgnoringBase(
            base.Update_where_keyless_entity_mapped_to_sql_query,
            async
        );

    [ConditionalTheory(Skip = "https://github.com/ydb-platform/ydb/issues/15177")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_where_using_hierarchy(bool async)
        => TestIgnoringBase(
            base.Delete_where_using_hierarchy,
            async
        );

    [ConditionalTheory(Skip = "https://github.com/ydb-platform/ydb/issues/15177")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_where_using_hierarchy_derived(bool async)
        => TestIgnoringBase(
            base.Delete_where_using_hierarchy_derived,
            async
        );

    public override Task Delete_GroupBy_Where_Select_First(bool async)
        => TestIgnoringBase(
            base.Delete_GroupBy_Where_Select_First,
            async
        );

    public override Task Delete_GroupBy_Where_Select_First_2(bool async)
        => TestIgnoringBase(
            base.Delete_GroupBy_Where_Select_First_2,
            async
        );

    [ConditionalTheory(Skip = "need fix")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_base_type(bool async)
        => TestIgnoringBase(
            base.Update_base_type,
            async
        );

    [ConditionalTheory(Skip = "need fix")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_base_type_with_OfType(bool async)
        => TestIgnoringBase(
            base.Update_base_type_with_OfType,
            async
        );

    public override Task Update_where_hierarchy_subquery(bool async)
        => TestIgnoringBase(
            base.Update_where_hierarchy_subquery,
            async
        );
    
    [ConditionalTheory(Skip = "need fix")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_base_property_on_derived_type(bool async)
        => TestIgnoringBase(
            base.Update_base_property_on_derived_type,
            async
        );

    [ConditionalTheory(Skip = "need fix")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_derived_property_on_derived_type(bool async)
        => TestIgnoringBase(
            base.Update_derived_property_on_derived_type,
            async
        );

    [ConditionalTheory(Skip = "https://github.com/ydb-platform/ydb/issues/15177")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_where_using_hierarchy(bool async)
        => TestIgnoringBase(
            base.Update_where_using_hierarchy,
            async
        );

    [ConditionalTheory(Skip = "https://github.com/ydb-platform/ydb/issues/15177")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_where_using_hierarchy_derived(bool async)
        => TestIgnoringBase(
            base.Update_where_using_hierarchy_derived,
            async
        );

    protected override void ClearLog()
        => Fixture.TestSqlLoggerFactory.Clear();

    private async Task TestIgnoringBase(
        Func<bool, Task> baseTest,
        bool async,
        params string[] expectedSql
    )
    {
        try
        {
            await baseTest(async);
        }
        catch (EqualException ex)
        {
            var commands = Fixture.TestSqlLoggerFactory.SqlStatements;
            var commandsStr = new StringBuilder();

            foreach (var command in commands)
            {
                commandsStr.Append("\n>>>\n");
                commandsStr.Append(command);
            }


            if (expectedSql.Length == 0) throw new AggregateException(ex, new Exception(commandsStr.ToString()));
            var actual = Fixture.TestSqlLoggerFactory.SqlStatements;
            for (var i = 0; i < expectedSql.Length; i++)
            {
                Assert.Equal(expectedSql[i], actual[i]);
            }
        }
        catch (Exception ex)
        {
            var commands = Fixture.TestSqlLoggerFactory.SqlStatements;
            var commandsStr = new StringBuilder();

            foreach (var command in commands)
            {
                commandsStr.Append("\n>>>\n");
                commandsStr.Append(command);
            }


            throw new AggregateException(new Exception($"Sql:{commandsStr}\n<<<\n"), ex);
        }
    }
}
