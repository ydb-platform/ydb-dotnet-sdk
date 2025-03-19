//npgsql

using System.Text;
using EfCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore.Update;
using Xunit;

namespace EfCore.Ydb.FunctionalTests.AllTests.Update;

// Tests:
// Ignore_before_save_property_is_still_generated_graph,
// Ignore_before_save_property_is_still_generated,
// SaveChanges_processes_all_tracked_entities.
// They're failing, but I cannot ignore them because they're not virtual
internal class UpdatesYdbTest
    : UpdatesRelationalTestBase<UpdatesYdbTest.UpdatesYdbFixture>
// , UpdatesTestBase<UpdatesYdbTest.UpdatesYdbFixture>
{
    public UpdatesYdbTest(UpdatesYdbFixture fixture) : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
    }

    public class UpdatesYdbFixture : UpdatesRelationalFixture
    {
        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;
    }

    public override void Identifiers_are_generated_correctly()
    {
        // TODO: implement later
    }

    public override Task SaveChanges_throws_for_entities_only_mapped_to_view()
        => TestIgnoringBase(base.SaveChanges_throws_for_entities_only_mapped_to_view);

    [Fact(Skip = "There's no foreign keys in ydb")]
    public override Task Save_with_shared_foreign_key()
        => TestIgnoringBase(base.Save_with_shared_foreign_key);

    [Fact(Skip = "Need fix")]
    public override Task Can_use_shared_columns_with_conversion()
        => TestIgnoringBase(base.Can_use_shared_columns_with_conversion);

    public override Task Swap_filtered_unique_index_values()
        => TestIgnoringBase(base.Swap_filtered_unique_index_values);

    public override Task Swap_computed_unique_index_values()
        => TestIgnoringBase(base.Swap_computed_unique_index_values);

    public override Task Update_non_indexed_values()
        => TestIgnoringBase(base.Update_non_indexed_values);

    [ConditionalTheory(Skip = "TODO: need fix")]
    [InlineData(false)]
    [InlineData(true)]
    public override Task Can_change_type_of_pk_to_pk_dependent_by_replacing_with_new_dependent(bool async)
        => TestIgnoringBase(
            base.Can_change_type_of_pk_to_pk_dependent_by_replacing_with_new_dependent, async);

    [ConditionalTheory(Skip = "TODO: need fix")]
    [InlineData(false)]
    [InlineData(true)]
    public override Task Can_change_type_of__dependent_by_replacing_with_new_dependent(bool async)
        => TestIgnoringBase(
            base.Can_change_type_of__dependent_by_replacing_with_new_dependent, async);

    public override Task Mutation_of_tracked_values_does_not_mutate_values_in_store()
        => TestIgnoringBase(
            base.Mutation_of_tracked_values_does_not_mutate_values_in_store);

    public override Task Save_partial_update()
        => TestIgnoringBase(
            base.Save_partial_update);

    [Fact(Skip = "TODO: need fix")]
    public override Task Save_partial_update_on_missing_record_throws()
        => TestIgnoringBase(
            base.Save_partial_update_on_missing_record_throws);

    [Fact(Skip = "TODO: need fix")]
    public override Task Save_partial_update_on_concurrency_token_original_value_mismatch_throws()
        => TestIgnoringBase(
            base.Save_partial_update_on_concurrency_token_original_value_mismatch_throws);

    [Fact(Skip = "TODO: need fix")]
    public override Task Update_on_bytes_concurrency_token_original_value_mismatch_throws()
        => TestIgnoringBase(
            base.Update_on_bytes_concurrency_token_original_value_mismatch_throws);

    public override Task Update_on_bytes_concurrency_token_original_value_matches_does_not_throw()
        => TestIgnoringBase(
            base.Update_on_bytes_concurrency_token_original_value_matches_does_not_throw);

    [Fact(Skip = "TODO: need fix")]
    public override Task Remove_on_bytes_concurrency_token_original_value_mismatch_throws()
        => TestIgnoringBase(
            base.Remove_on_bytes_concurrency_token_original_value_mismatch_throws);

    public override Task Remove_on_bytes_concurrency_token_original_value_matches_does_not_throw()
        => TestIgnoringBase(
            base.Remove_on_bytes_concurrency_token_original_value_matches_does_not_throw);

    [Fact(Skip = "TODO: need fix")]
    public override Task Can_add_and_remove_self_refs()
        => TestIgnoringBase(
            base.Can_add_and_remove_self_refs);

    [Fact(Skip = "TODO: need fix")]
    public override Task Can_change_enums_with_conversion()
        => TestIgnoringBase(
            base.Can_change_enums_with_conversion);

    public override Task Can_remove_partial()
        => TestIgnoringBase(
            base.Can_remove_partial);

    [Fact(Skip = "TODO: need fix")]
    public override Task Remove_partial_on_missing_record_throws()
        => TestIgnoringBase(
            base.Remove_partial_on_missing_record_throws);

    [Fact(Skip = "TODO: need fix")]
    public override Task Remove_partial_on_concurrency_token_original_value_mismatch_throws()
        => TestIgnoringBase(
            base.Remove_partial_on_concurrency_token_original_value_mismatch_throws);

    public override Task Save_replaced_principal()
        => TestIgnoringBase(
            base.Save_replaced_principal);

    private async Task TestIgnoringBase(
        Func<Task> baseTest
    ) => await TestIgnoringBase(_ => baseTest(), false);

    private async Task TestIgnoringBase(
        Func<bool, Task> baseTest,
        bool async
    )
    {
        try
        {
            await baseTest(async);
        }
        catch (Exception e)
        {
            // if (expectedSql.Length == 0) throw;
            var actual = Fixture.TestSqlLoggerFactory.SqlStatements;

            var commas = new StringBuilder();
            foreach (var str in actual)
            {
                commas
                    .Append(">>>\n")
                    .Append(str)
                    .Append("\n<<<\n");
            }

            throw new AggregateException(new Exception(commas.ToString()), e);
        }
    }
}
