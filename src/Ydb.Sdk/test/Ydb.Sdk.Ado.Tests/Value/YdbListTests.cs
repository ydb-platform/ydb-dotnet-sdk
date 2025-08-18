using Xunit;
using Ydb.Sdk.Ado.YdbType;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.Tests.Value;

public class YdbListTests : TestBase
{
    [Fact]
    public void ListOfInt64_FromPlainObjects_IsInferred()
    {
        var param = new YdbParameter("$ids", new YdbList([1L, 2L, 3L]));
        var tv = param.TypedValue;

        Assert.Equal(3, tv.Value.Items.Count);
        Assert.All(tv.Value.Items, v => Assert.Equal(Ydb.Value.ValueOneofCase.Int64Value, v.ValueCase));

        Assert.Equal(1L, tv.Value.Items[0].Int64Value);
        Assert.Equal(2L, tv.Value.Items[1].Int64Value);
        Assert.Equal(3L, tv.Value.Items[2].Int64Value);
    }

    [Fact]
    public void ListOfUtf8_FromPlainStrings_IsInferred()
    {
        var param = new YdbParameter("$tags", new YdbList(["a", "b"]));
        var tv = param.TypedValue;

        Assert.Equal(2, tv.Value.Items.Count);
        Assert.All(tv.Value.Items, v => Assert.Equal(Ydb.Value.ValueOneofCase.TextValue, v.ValueCase));

        Assert.Equal("a", tv.Value.Items[0].TextValue);
        Assert.Equal("b", tv.Value.Items[1].TextValue);
    }

    [Fact]
    public void ListOfStruct_WithNestedList_HasExpectedShape()
    {
        // List<Struct<Key:Int64,SubKey:Int64,Value:Utf8,Tags:List<Utf8>>>
        var rows = new YdbList(new object[]
        {
            YdbValue.MakeStruct(new Dictionary<string, YdbValue>
            {
                ["Key"]    = YdbValue.MakeInt64(1),
                ["SubKey"] = YdbValue.MakeInt64(2),
                ["Value"]  = YdbValue.MakeUtf8("v"),
                ["Tags"]   = YdbValue.MakeList([YdbValue.MakeUtf8("a"), YdbValue.MakeUtf8("b")])
            }),
            YdbValue.MakeStruct(new Dictionary<string, YdbValue>
            {
                ["Key"]    = YdbValue.MakeInt64(10),
                ["SubKey"] = YdbValue.MakeInt64(20),
                ["Value"]  = YdbValue.MakeUtf8("vv"),
                ["Tags"]   = YdbValue.MakeList([YdbValue.MakeUtf8("x")])
            })
        });

        var p = new YdbParameter("$rows", rows);
        var tv = p.TypedValue;

        Assert.Equal(2, tv.Value.Items.Count);

        var row1 = tv.Value.Items[0].Items;
        Assert.Equal(4, row1.Count);

        Assert.Equal(Ydb.Value.ValueOneofCase.Int64Value, row1[0].ValueCase);
        Assert.Equal(Ydb.Value.ValueOneofCase.Int64Value, row1[1].ValueCase);
        Assert.Equal(Ydb.Value.ValueOneofCase.TextValue,  row1[2].ValueCase);

        Assert.Equal(Ydb.Value.ValueOneofCase.None, row1[3].ValueCase);
        Assert.True(row1[3].Items.Count > 0);

        Assert.Equal(1L, row1[0].Int64Value);
        Assert.Equal(2L, row1[1].Int64Value);
        Assert.Equal("v", row1[2].TextValue);

        var tags1 = row1[3].Items;
        Assert.Equal(2, tags1.Count);
        Assert.Equal("a", tags1[0].TextValue);
        Assert.Equal("b", tags1[1].TextValue);

        var row2 = tv.Value.Items[1].Items;
        Assert.Equal(4, row2.Count);
        Assert.Equal(10L, row2[0].Int64Value);
        Assert.Equal(20L, row2[1].Int64Value);
        Assert.Equal("vv", row2[2].TextValue);

        var tags2 = row2[3].Items;
        Assert.Single(tags2);
        Assert.Equal("x", tags2[0].TextValue);
    }

    [Fact]
    public void List_MixedItems_YdbValue_YdbParameter_Primitives_AreUnified()
    {
        var mixed = new YdbList(new object[]
        {
            YdbValue.MakeInt64(1),
            new YdbParameter { YdbDbType = YdbDbType.Int64, Value = 2L },
            3L
        });

        var p = new YdbParameter("$mixed", mixed);
        var tv = p.TypedValue;

        Assert.Equal(3, tv.Value.Items.Count);
        Assert.All(tv.Value.Items, v => Assert.Equal(Ydb.Value.ValueOneofCase.Int64Value, v.ValueCase));

        Assert.Equal(1L, tv.Value.Items[0].Int64Value);
        Assert.Equal(2L, tv.Value.Items[1].Int64Value);
        Assert.Equal(3L, tv.Value.Items[2].Int64Value);
    }

    [Fact]
    public void UPDATE_ON_SELECT_YdbList_Shape_And_SampleYql()
    {
        // $to_update: List<Struct<Id:Int64, Value:Utf8>>
        var toUpdate = new YdbList(new object[]
        {
            YdbValue.MakeStruct(new Dictionary<string, YdbValue>
            {
                ["Id"] = YdbValue.MakeInt64(1),
                ["Value"] = YdbValue.MakeUtf8("new-1")
            }),
            YdbValue.MakeStruct(new Dictionary<string, YdbValue>
            {
                ["Id"] = YdbValue.MakeInt64(2),
                ["Value"] = YdbValue.MakeUtf8("new-2")
            }),
        });

        var p = new YdbParameter("$to_update", toUpdate);
        var tv = p.TypedValue;

        Assert.Equal(2, tv.Value.Items.Count);
        foreach (var row in tv.Value.Items)
        {
            Assert.Equal(2, row.Items.Count);
            Assert.Equal(Ydb.Value.ValueOneofCase.Int64Value, row.Items[0].ValueCase); // Id
            Assert.Equal(Ydb.Value.ValueOneofCase.TextValue,  row.Items[1].ValueCase); // Value
        }

        const string yql = """
            UPDATE my_table ON
            SELECT * FROM $to_update;
            """;
        Assert.Contains("UPDATE my_table ON", yql);
        Assert.Contains("SELECT * FROM $to_update;", yql);
    }

    [Fact]
    public void DELETE_ON_SELECT_YdbListPk_Shape_And_SampleYql()
    {
        // $to_delete: List<Struct<Id:Int64>>
        var toDelete = new YdbList(new object[]
        {
            YdbValue.MakeStruct(new Dictionary<string, YdbValue> { ["Id"] = YdbValue.MakeInt64(1) }),
            YdbValue.MakeStruct(new Dictionary<string, YdbValue> { ["Id"] = YdbValue.MakeInt64(2) }),
        });

        var p = new YdbParameter("$to_delete", toDelete);
        var tv = p.TypedValue;

        Assert.Equal(2, tv.Value.Items.Count);
        foreach (var row in tv.Value.Items)
        {
            Assert.Single(row.Items);
            Assert.Equal(Ydb.Value.ValueOneofCase.Int64Value, row.Items[0].ValueCase);
        }

        const string yql = """
            DELETE my_table ON
            SELECT * FROM $to_delete;
            """;
        Assert.Contains("DELETE my_table ON", yql);
        Assert.Contains("SELECT * FROM $to_delete;", yql);
    }

    [Fact]
    public void INSERT_INTO_SELECT_YdbList_Shape_And_SampleYql()
    {
        // $rows: List<Struct<Id:Int64, Value:Utf8>>
        var rows = new YdbList(new object[]
        {
            YdbValue.MakeStruct(new Dictionary<string, YdbValue>
            {
                ["Id"] = YdbValue.MakeInt64(10),
                ["Value"] = YdbValue.MakeUtf8("v")
            })
        });

        var p = new YdbParameter("$rows", rows);
        var tv = p.TypedValue;

        Assert.Single(tv.Value.Items);
        Assert.Equal(2, tv.Value.Items[0].Items.Count);
        Assert.Equal(Ydb.Value.ValueOneofCase.Int64Value, tv.Value.Items[0].Items[0].ValueCase); // Id
        Assert.Equal(Ydb.Value.ValueOneofCase.TextValue,  tv.Value.Items[0].Items[1].ValueCase); // Value

        const string yql = """
            INSERT INTO my_table
            SELECT * FROM $rows;
            """;
        Assert.Contains("INSERT INTO my_table", yql);
        Assert.DoesNotContain(" ON", yql);
    }

    [Fact]
    public void UPSERT_INTO_SELECT_YdbList_Shape_And_SampleYql()
    {
        // $rows: List<Struct<Id:Int64, Value:Utf8>>
        var rows = new YdbList(new object[]
        {
            YdbValue.MakeStruct(new Dictionary<string, YdbValue>
            {
                ["Id"] = YdbValue.MakeInt64(10),
                ["Value"] = YdbValue.MakeUtf8("vv")
            })
        });

        var p = new YdbParameter("$rows", rows);
        var tv = p.TypedValue;

        Assert.Single(tv.Value.Items);
        Assert.Equal(2, tv.Value.Items[0].Items.Count);
        Assert.Equal(Ydb.Value.ValueOneofCase.Int64Value, tv.Value.Items[0].Items[0].ValueCase); // Id
        Assert.Equal(Ydb.Value.ValueOneofCase.TextValue,  tv.Value.Items[0].Items[1].ValueCase); // Value

        const string yql = """
            UPSERT INTO my_table
            SELECT * FROM $rows;
            """;
        Assert.Contains("UPSERT INTO my_table", yql);
        Assert.DoesNotContain(" ON", yql);
    }
    
    [Fact]
    public async Task Update_On_ListStruct_UpdatesRows()
    {
        var table = $"UpdOn_{Guid.NewGuid():N}";
        await using var conn = await CreateOpenConnectionAsync();
        try
        {
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $@"
                    CREATE TABLE {table} (
                        Id Int64,
                        Value Utf8,
                        Other Utf8,
                        PRIMARY KEY (Id)
                    );";
                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = $@"UPSERT INTO {table} (Id, Value, Other) VALUES
                    (1, 'old-1', 'keep'),
                    (2, 'old-2', 'keep'),
                    (3, 'old-3', 'keep');";
                await cmd.ExecuteNonQueryAsync();
            }

            // $to_update : List<Struct<Id:Int64, Value:Utf8>>
            var toUpdate = new YdbList(new object[]
            {
                YdbValue.MakeStruct(new Dictionary<string, YdbValue>
                {
                    ["Id"] = YdbValue.MakeInt64(1),
                    ["Value"] = YdbValue.MakeUtf8("new-1")
                }),
                YdbValue.MakeStruct(new Dictionary<string, YdbValue>
                {
                    ["Id"] = YdbValue.MakeInt64(3),
                    ["Value"] = YdbValue.MakeUtf8("new-3")
                })
            });

            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $@"
                    DECLARE $to_update AS List<Struct<Id:Int64, Value:Utf8>>;
                    UPDATE {table} ON
                    SELECT * FROM AS_TABLE($to_update);";
                cmd.Parameters.Add(new YdbParameter("$to_update", toUpdate));
                await cmd.ExecuteNonQueryAsync();
            }

            await using (var check = conn.CreateCommand())
            {
                check.CommandText = $"SELECT Id, Value, Other FROM {table} ORDER BY Id;";
                var rows = new List<(long id, string val, string oth)>();
                await using var r = await check.ExecuteReaderAsync();
                while (await r.ReadAsync())
                    rows.Add((r.GetInt64(0), r.GetString(1), r.GetString(2)));

                Assert.Equal((1L, "new-1", "keep"), rows[0]);
                Assert.Equal((2L, "old-2", "keep"), rows[1]);
                Assert.Equal((3L, "new-3", "keep"), rows[2]);
            }
        }
        finally
        {
            await using var drop = conn.CreateCommand();
            drop.CommandText = $"DROP TABLE {table}";
            await drop.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task Delete_On_ListStruct_DeletesByPk()
    {
        var table = $"DelOn_{Guid.NewGuid():N}";
        await using var conn = await CreateOpenConnectionAsync();
        try
        {
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $@"
                    CREATE TABLE {table} (
                        Id Int64,
                        Value Utf8,
                        PRIMARY KEY (Id)
                    );";
                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = $@"UPSERT INTO {table} (Id, Value) VALUES
                    (1, 'a'), (2, 'b'), (3, 'c');";
                await cmd.ExecuteNonQueryAsync();
            }

            // $to_delete : List<Struct<Id:Int64>>
            var toDelete = new YdbList(new object[]
            {
                YdbValue.MakeStruct(new Dictionary<string, YdbValue> { ["Id"] = YdbValue.MakeInt64(1) }),
                YdbValue.MakeStruct(new Dictionary<string, YdbValue> { ["Id"] = YdbValue.MakeInt64(3) })
            });

            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $@"
        DECLARE $to_delete AS List<Struct<Id:Int64>>;
        DELETE FROM {table} ON
        SELECT Id FROM AS_TABLE($to_delete);";
                cmd.Parameters.Add(new YdbParameter("$to_delete", toDelete));
                await cmd.ExecuteNonQueryAsync();
            }


            await using (var check = conn.CreateCommand())
            {
                check.CommandText = $"SELECT ARRAY_LENGTH(AsList()) FROM (SELECT * FROM {table});";
                check.CommandText = $"SELECT COUNT(*) FROM {table};";
                var left = Convert.ToInt32(await check.ExecuteScalarAsync());
                Assert.Equal(1, left);
            }
        }
        finally
        {
            await using var drop = conn.CreateCommand();
            drop.CommandText = $"DROP TABLE {table}";
            await drop.ExecuteNonQueryAsync();
        }
    }
}
