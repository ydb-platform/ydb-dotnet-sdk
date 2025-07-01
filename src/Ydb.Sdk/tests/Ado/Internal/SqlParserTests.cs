using Xunit;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Tests.Ado.Internal;

public class SqlParserTests
{
    [Fact]
    public void Parse_WhenDigitOrLetterOrUnderliningInParamName_ReturnParsedSqlAndCached()
    {
        var (sql, sqlParams) = SqlParser.Parse("SELECT @p; SELECT @p2; SELECT @p_3;");

        Assert.Equal("SELECT $p; SELECT $p2; SELECT $p_3;", sql);
        Assert.Equal(new PrimitiveParam[] { new("$p"), new("$p2"), new("$p_3") }, sqlParams);
    }

    [Fact]
    public void Parse_WhenSelectMultiLineString_ReturnSqlWithoutParams()
    {
        var (sql, sqlParams) = SqlParser.Parse("SELECT @@p  ; SELECT @@/* comment with @p */");
        Assert.Equal("SELECT @@p  ; SELECT @@/* comment with @p */", sql);
        Assert.Empty(sqlParams);
    }

    [Fact]
    public void Parse_WhenEmptySql_ReturnEmpty()
    {
        var (sql, paramsNames) = SqlParser.Parse("");
        Assert.Equal("", sql);
        Assert.True(paramsNames.Count == 0);
    }

    [Fact]
    public void Parse_WhenLineComment_ReturnSqlWithComment()
    {
        var (sql, sqlParams) = SqlParser.Parse(@"-- Comment with params @param, @p2, @p_3
SELECT @param; SELECT @p2; SELECT @p_3;");

        Assert.Equal(@"-- Comment with params @param, @p2, @p_3
SELECT $param; SELECT $p2; SELECT $p_3;", sql);
        Assert.Equal(new PrimitiveParam[] { new("$param"), new("$p2"), new("$p_3") }, sqlParams);
    }

    [Fact]
    public void Parse_WhenBlockComment_ReturnSqlWithComment()
    {
        var (sql, sqlParams) = SqlParser.Parse(@"/* Comment SQL
/*
Comment with params @param, @p2, @p_3
--
@
*/
SELECT @param; SELECT @p2; SELECT @p_3
*/
INSERT INTO Table 
    (id, bool_column, bigint_column, smallint_column, tinyint_column, float_column, double_column, decimal_column, 
     uint8_column, uint16_column, uint32_column, uint64_column, text_column, binary_column, json_column,
     jsondocument_column, date_column, datetime_column, timestamp_column, interval_column) VALUES
(@name1, @name2, @name3, @name4, @name5, @name6, @name7, @name8, @name9, @name10, @name11, @name12, @name13, @name14,
 @name15, @name16, @name17, @name18, @name19, @name20);");

        Assert.Equal(@"/* Comment SQL
/*
Comment with params @param, @p2, @p_3
--
@
*/
SELECT @param; SELECT @p2; SELECT @p_3
*/
INSERT INTO Table 
    (id, bool_column, bigint_column, smallint_column, tinyint_column, float_column, double_column, decimal_column, 
     uint8_column, uint16_column, uint32_column, uint64_column, text_column, binary_column, json_column,
     jsondocument_column, date_column, datetime_column, timestamp_column, interval_column) VALUES
($name1, $name2, $name3, $name4, $name5, $name6, $name7, $name8, $name9, $name10, $name11, $name12, $name13, $name14,
 $name15, $name16, $name17, $name18, $name19, $name20);", sql);
        Assert.Equal(new PrimitiveParam[]
        {
            new("$name1"), new("$name2"), new("$name3"), new("$name4"), new("$name5"),
            new("$name6"), new("$name7"), new("$name8"), new("$name9"), new("$name10"),
            new("$name11"), new("$name12"), new("$name13"), new("$name14"), new("$name15"),
            new("$name16"), new("$name17"), new("$name18"), new("$name19"), new("$name20")
        }, sqlParams);
    }

    [Fact]
    public void Parse_WhenDoubleQuotes_ReturnSql()
    {
        var (sql, sqlParams) = SqlParser.Parse(@"REPLACE INTO episodes
(
    series_id,
    season_id,
    episode_id,
    title,
    air_date
)
VALUES
(
    2,
    5,
    12,
    ""@ @ @ @ @ @Test Episode !!! \"" \"" \"" \"" SELECT @param; SELECT @p2; SELECT @p_3"",
    @air_date
)

COMMIT;

-- View result:
SELECT * FROM episodes WHERE series_id = ""123 @ \"" @ @"" AND season_id = @param;
;");
        Assert.Equal(@"REPLACE INTO episodes
(
    series_id,
    season_id,
    episode_id,
    title,
    air_date
)
VALUES
(
    2,
    5,
    12,
    ""@ @ @ @ @ @Test Episode !!! \"" \"" \"" \"" SELECT @param; SELECT @p2; SELECT @p_3"",
    $air_date
)

COMMIT;

-- View result:
SELECT * FROM episodes WHERE series_id = ""123 @ \"" @ @"" AND season_id = $param;
;", sql);
        Assert.Equal(new PrimitiveParam[] { new("$air_date"), new("$param") }, sqlParams);
    }

    [Fact]
    public void Parse_WhenSingleQuotes_ReturnSql()
    {
        var (sql, sqlParams) = SqlParser.Parse(@"UPSERT INTO episodes
(
    series_id,
    season_id,
    episode_id,
    title,
    air_date
)
VALUES
(
    2,
    5,
    13,
    'Test Episode @ \'@ @ \'@ \'@ \'@ @ @ @ @ @ @ @ ',
    @air_date
)
;

COMMIT;

-- View result:
SELECT * FROM episodes WHERE series_id = '123 @ \' @ @' AND season_id = @param;");
        Assert.Equal(@"UPSERT INTO episodes
(
    series_id,
    season_id,
    episode_id,
    title,
    air_date
)
VALUES
(
    2,
    5,
    13,
    'Test Episode @ \'@ @ \'@ \'@ \'@ @ @ @ @ @ @ @ ',
    $air_date
)
;

COMMIT;

-- View result:
SELECT * FROM episodes WHERE series_id = '123 @ \' @ @' AND season_id = $param;", sql);
        Assert.Equal(new PrimitiveParam[] { new("$air_date"), new("$param") }, sqlParams);
    }

    [Fact]
    public void Parse_WhenBacktickQuotes_ReturnSql()
    {
        var (sql, sqlParams) = SqlParser.Parse(@"UPSERT INTO `episodes @ @ @ @  "" "" \` @ @ @`
(
    series_id,
    season_id,
    episode_id,
    title,
    air_date
)
VALUES
(
    2,
    5,
    13,
    'Test Episode @ \'@ @ \'@ \'@ \'@ @ @ @ @ @ @ @ ',
    @air_date
)
;

COMMIT;

-- View result:
SELECT * FROM episodes WHERE series_id = '123 @ \' @ @' AND season_id = @param;");

        Assert.Equal(@"UPSERT INTO `episodes @ @ @ @  "" "" \` @ @ @`
(
    series_id,
    season_id,
    episode_id,
    title,
    air_date
)
VALUES
(
    2,
    5,
    13,
    'Test Episode @ \'@ @ \'@ \'@ \'@ @ @ @ @ @ @ @ ',
    $air_date
)
;

COMMIT;

-- View result:
SELECT * FROM episodes WHERE series_id = '123 @ \' @ @' AND season_id = $param;", sql);
        Assert.Equal(new PrimitiveParam[] { new("$air_date"), new("$param") }, sqlParams);
    }

    [Fact]
    public void Parse_WhenMultilineStringLiterals_ReturnSql()
    {
        var (sql, sqlParams) = SqlParser.Parse(@"$text = @@some
multiline with double at: @@@@
text@@;
SELECT $text;
-- Comment with params @param, @p2, @p_3
SELECT @param; SELECT @p2; SELECT @p_3;");

        Assert.Equal(@"$text = @@some
multiline with double at: @@@@
text@@;
SELECT $text;
-- Comment with params @param, @p2, @p_3
SELECT $param; SELECT $p2; SELECT $p_3;", sql);
        Assert.Equal(new PrimitiveParam[] { new("$param"), new("$p2"), new("$p_3") }, sqlParams);
    }

    [Fact]
    public void Parse_WhenRepeatedOneParam_ReturnThisParamInParamNames()
    {
        var (sql, sqlParams) = SqlParser.Parse("SELECT @a, @a, @a;");

        Assert.Equal("SELECT $a, $a, $a;", sql);
        Assert.Equal(new PrimitiveParam[] { new("$a") }, sqlParams);
    }

    [Fact]
    public void Parse_WhenParamHasNextStatementIsNotWhitespace_ReturnSql()
    {
        var (sql, sqlParams) = SqlParser.Parse("SELECT @a/* this comment @ */");

        Assert.Equal("SELECT $a/* this comment @ */", sql);
        Assert.Equal(new PrimitiveParam[] { new("$a") }, sqlParams);
    }

    [Theory]
    [InlineData("SELECT a FROM Table WHERE b IN $Gen_List_Primitive_1", "SELECT a FROM Table WHERE b IN (@id1)", 1)]
    [InlineData("SELECT a FROM Table WHERE b IN $Gen_List_Primitive_1",
        "SELECT a FROM Table WHERE b IN (@id1, @id2, @id3, @id4, @id5)", 5)]
    [InlineData("SELECT a FROM Table WHERE b IN $Gen_List_Primitive_1",
        "SELECT a FROM Table WHERE b IN (/* comment in list parameters */ @id1, @id2, @id3, @id4)", 4)]
    [InlineData("SELECT a FROM Table WHERE b IN $Gen_List_Primitive_1;",
        "SELECT a FROM Table WHERE b IN (    @id1, @id2,   @id3, -- asdasdaksld\n @id4   );", 4)]
    [InlineData("SELECT a FROM Table WHERE b IN " +
                "/* comment in list parameters */$Gen_List_Primitive_1/* comment in list parameters */;",
        "SELECT a FROM Table WHERE b IN /* comment in list parameters */(" +
        "/* comment in list parameters */@id1,/* comment in list parameters */@id2," +
        "/* comment in list parameters */@id3/* comment in list parameters */)" +
        "/* comment in list parameters */;", 3)]
    public void Parse_WhenInListParameters_ReturnSqlWithListParam(string expectedSql, string actualSql, int listSize)
    {
        var (sql, sqlParams) = SqlParser.Parse(actualSql);
        Assert.Equal(expectedSql, sql);

        var ydbParameters = new Dictionary<string, YdbValue>();
        for (var i = 1; i <= listSize; i++)
        {
            ydbParameters[$"$id{i}"] = YdbValue.MakeInt32(i);
        }

        var listYdbValues = sqlParams[0].YdbValueFetch(ydbParameters).GetList();

        Assert.Equal(listSize, listYdbValues.Count);
        for (var i = 1; i <= listSize; i++)
        {
            Assert.Equal(i, listYdbValues[i - 1].GetInt32());
        }
    }

    [Theory]
    [InlineData("SELECT a FROM Table WHERE b IN ($id1, \"abacaba\")",
        "SELECT a FROM Table WHERE b IN (@id1, \"abacaba\")")]
    [InlineData("SELECT a FROM Table WHERE b IN ($id1, 'abacaba')",
        "SELECT a FROM Table WHERE b IN (@id1, 'abacaba')")]
    [InlineData("SELECT a FROM Table WHERE b IN ($id1, @@asdasd@@)",
        "SELECT a FROM Table WHERE b IN (@id1, @@asdasd@@)")]
    [InlineData("SELECT a FROM Table WHERE b IN ($id1, \"abacaba\",  $id2)",
        "SELECT a FROM Table WHERE b IN (@id1, \"abacaba\",  $id2)")]
    [InlineData("SELECT a FROM Table WHERE b IN ($id1, 'abacaba',  $id2)",
        "SELECT a FROM Table WHERE b IN (@id1, 'abacaba',  $id2)")]
    [InlineData("SELECT a FROM Table WHERE b IN ($id1, @@asdasd@@,  $id2)",
        "SELECT a FROM Table WHERE b IN (@id1, @@asdasd@@,  $id2)")]
    public void Parse_WhenListParametersWithPrimitives_ReturnSqlWithoutListParam(string expectedSql, string actualSql)
    {
        var (sql, sqlParams) = SqlParser.Parse(actualSql);
        Assert.Equal(expectedSql, sql);
        Assert.True(sqlParams.All(param => param is not ListPrimitiveParam));
    }

    [Fact]
    public void Parse_WhenManySqlStatementWithListParameters_ReturnSqlWithListParam()
    {
        var (sql, sqlParams) = SqlParser.Parse(@"
SELECT a, b, c FROM Table WHERE d IN (@id1, @id2, @id3, @id4, @id5); -- first list
SELECT @simple_param_first;
SELECT a, b, c FROM Table WHERE d IN (@id1, @id2, @id3, @id4); -- second list
SELECT a FROM Table WHERE b IN (@id1, '@@asdasd@@',  @id2); -- third list
SELECT @simple_param_second;
");
        Assert.Equal(@"
SELECT a, b, c FROM Table WHERE d IN $Gen_List_Primitive_1; -- first list
SELECT $simple_param_first;
SELECT a, b, c FROM Table WHERE d IN $Gen_List_Primitive_2; -- second list
SELECT a FROM Table WHERE b IN ($id1, '@@asdasd@@',  $id2); -- third list
SELECT $simple_param_second;
", sql);

        Assert.Equal(6, sqlParams.Count);
        var ydbParameters = new Dictionary<string, YdbValue>();
        for (var i = 1; i <= 5; i++)
        {
            ydbParameters[$"$id{i}"] = YdbValue.MakeInt32(i);
        }

        ydbParameters["$simple_param_first"] = YdbValue.MakeUtf8("first");
        ydbParameters["$simple_param_second"] = YdbValue.MakeUtf8("second");

        Assert.Equal("$Gen_List_Primitive_1", sqlParams[0].Name);
        var listPrimitive1 = sqlParams[0].YdbValueFetch(ydbParameters).GetList();
        Assert.Equal(5, listPrimitive1.Count);
        for (var i = 0; i < 5; i++)
        {
            Assert.Equal(i + 1, listPrimitive1[i].GetInt32());    
        }
        
        Assert.Equal("$Gen_List_Primitive_2", sqlParams[2].Name);
        var listPrimitive2 = sqlParams[2].YdbValueFetch(ydbParameters).GetList();
        Assert.Equal(4, listPrimitive2.Count);
        for (var i = 0; i < 4; i++)
        {
            Assert.Equal(i + 1, listPrimitive2[i].GetInt32());    
        }
        
        Assert.Equal("first", sqlParams[1].YdbValueFetch(ydbParameters).GetUtf8());
        Assert.Equal("second", sqlParams[5].YdbValueFetch(ydbParameters).GetUtf8());
        Assert.Equal(1, sqlParams[3].YdbValueFetch(ydbParameters).GetInt32());
        Assert.Equal(2, sqlParams[4].YdbValueFetch(ydbParameters).GetInt32());
    }
}
