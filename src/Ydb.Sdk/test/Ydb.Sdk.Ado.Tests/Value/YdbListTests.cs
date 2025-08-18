using Xunit;
using Ydb.Sdk.Ado.YdbType;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.Tests.Value;

public class YdbListTests : TestBase
{
    [Fact]
    public void Struct_BasicShape_ProducesListOfStruct()
    {
        // $rows: List<Struct<Id:Int64, Value:Utf8>>
        var rows = YdbList
            .Struct("Id", "Value")
            .AddRow(1L, "a")
            .AddRow(2L, "b");

        var tv = new YdbParameter("$rows", rows).TypedValue;

        Assert.Equal(2, tv.Value.Items.Count);

        var r1 = tv.Value.Items[0].Items;
        Assert.Equal(2, r1.Count);
        Assert.Equal(Ydb.Value.ValueOneofCase.Int64Value, r1[0].ValueCase); // Id
        Assert.Equal(Ydb.Value.ValueOneofCase.TextValue,  r1[1].ValueCase); // Value
        Assert.Equal(1L, r1[0].Int64Value);
        Assert.Equal("a", r1[1].TextValue);

        var r2 = tv.Value.Items[1].Items;
        Assert.Equal(2, r2.Count);
        Assert.Equal(Ydb.Value.ValueOneofCase.Int64Value, r2[0].ValueCase);
        Assert.Equal(Ydb.Value.ValueOneofCase.TextValue,  r2[1].ValueCase);
        Assert.Equal(2L, r2[0].Int64Value);
        Assert.Equal("b", r2[1].TextValue);
    }

    [Fact]
    public void Struct_AllNonNullThenNull_UsesNullFlagValue()
    {
        var rows = YdbList
            .Struct(["Id", "Name"], [YdbDbType.Int64, YdbDbType.Text])
            .AddRow(1L, "A")
            .AddRow(2L, null);

        var tv = new YdbParameter("$rows", rows).TypedValue;

        Assert.Equal(2, tv.Value.Items.Count);

        var r2 = tv.Value.Items[1].Items;
        Assert.Equal(2, r2.Count);
        Assert.Equal(Ydb.Value.ValueOneofCase.Int64Value, r2[0].ValueCase);
        Assert.Equal(2L, r2[0].Int64Value);

        Assert.Equal(Ydb.Value.ValueOneofCase.NullFlagValue, r2[1].ValueCase);
    }

    [Fact]
    public void Struct_NullBeforeInference_Throws()
    {
        var rows = YdbList
            .Struct("Id", "Value")
            .AddRow(1L, null)
            .AddRow(2L, "B");

        var p = new YdbParameter("$rows", rows);
        var ex = Assert.Throws<InvalidOperationException>(() => { var _ = p.TypedValue; });

        Assert.Contains("null", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("explicit", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Struct_WithTypeHints_AllowsTypedNulls()
    {
        var rows = YdbList.Struct(
                ["Id", "Name"],
                [YdbDbType.Int64, YdbDbType.Text])
            .AddRow(1L, "A")
            .AddRow(2L, null);

        var tv = new YdbParameter("$rows", rows).TypedValue;

        Assert.Equal(2, tv.Value.Items.Count);

        var r2 = tv.Value.Items[1].Items;
        Assert.Equal(2, r2.Count);
        Assert.Equal(Ydb.Value.ValueOneofCase.Int64Value, r2[0].ValueCase);
        Assert.Equal(2L, r2[0].Int64Value);

        Assert.Equal(Ydb.Value.ValueOneofCase.NullFlagValue, r2[1].ValueCase);
    }

    [Fact]
    public void Struct_EmptyWithoutTypeHints_Throws()
    {
        var rows = YdbList.Struct("Id", "Value");
        var p = new YdbParameter("$rows", rows);

        var ex = Assert.Throws<InvalidOperationException>(() => { var _ = p.TypedValue; });
        Assert.Contains("infer", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Struct_NullWithoutAnyNonNull_InColumn_Throws()
    {
        var rows = YdbList
            .Struct("Id", "Value")
            .AddRow(1L, null);

        var p = new YdbParameter("$rows", rows);
        var ex = Assert.Throws<InvalidOperationException>(() => { var _ = p.TypedValue; });

        Assert.Contains("only null", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("explicit", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Struct_AddRow_WrongArity_Throws()
    {
        var rows = YdbList.Struct("Id", "Value");

        var ex1 = Assert.Throws<ArgumentException>(() => rows.AddRow(1L));
        Assert.Contains("Expected 2 values", ex1.Message, StringComparison.OrdinalIgnoreCase);

        var ex2 = Assert.Throws<ArgumentException>(() => rows.AddRow(1L, "a", 123));
        Assert.Contains("Expected 2 values", ex2.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void PlainMode_BackCompat_ListOfPrimitives()
    {
        var plain = new YdbList([1L, 2L, 3L]);
        var tv = new YdbParameter("$ids", plain).TypedValue;

        Assert.Equal(3, tv.Value.Items.Count);
        Assert.All(tv.Value.Items, v => Assert.Equal(Ydb.Value.ValueOneofCase.Int64Value, v.ValueCase));

        Assert.Equal(1L, tv.Value.Items[0].Int64Value);
        Assert.Equal(2L, tv.Value.Items[1].Int64Value);
        Assert.Equal(3L, tv.Value.Items[2].Int64Value);
    }

    [Fact]
    public void Shape_For_Update_Delete_Insert_Upsert_Samples()
    {
        var toUpdate = YdbList.Struct("Id", "Value")
            .AddRow(1L, "new-1")
            .AddRow(2L, "new-2");
        var pUpdate = new YdbParameter("$to_update", toUpdate).TypedValue;
        Assert.Equal(2, pUpdate.Value.Items.Count);
        Assert.True(pUpdate.Value.Items.All(r => r.Items.Count == 2));

        var toDelete = YdbList.Struct("Id")
            .AddRow(1L)
            .AddRow(3L);
        var pDelete = new YdbParameter("$to_delete", toDelete).TypedValue;
        Assert.Equal(2, pDelete.Value.Items.Count);
        Assert.True(pDelete.Value.Items.All(r => r.Items.Count == 1));

        const string yqlUpdate = """
            UPDATE my_table ON
            SELECT * FROM $to_update;
            """;
        Assert.Contains("UPDATE my_table ON", yqlUpdate);

        const string yqlDelete = """
            DELETE my_table ON
            SELECT * FROM $to_delete;
            """;
        Assert.Contains("DELETE my_table ON", yqlDelete);

        var insertRows = YdbList.Struct("Id", "Value").AddRow(10L, "v");
        var pInsert = new YdbParameter("$rows", insertRows).TypedValue;
        Assert.Single(pInsert.Value.Items);
        const string yqlInsert = """
            INSERT INTO my_table
            SELECT * FROM $rows;
            """;
        Assert.Contains("INSERT INTO my_table", yqlInsert);
        Assert.DoesNotContain(" ON", yqlInsert);

        var upsertRows = YdbList.Struct("Id", "Value").AddRow(10L, "vv");
        var pUpsert = new YdbParameter("$rows", upsertRows).TypedValue;
        Assert.Single(pUpsert.Value.Items);
        const string yqlUpsert = """
            UPSERT INTO my_table
            SELECT * FROM $rows;
            """;
        Assert.Contains("UPSERT INTO my_table", yqlUpsert);
        Assert.DoesNotContain(" ON", yqlUpsert);
    }
}
