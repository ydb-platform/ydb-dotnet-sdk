using Xunit;
using Ydb.Sdk.Ado.Internal;

namespace Ydb.Sdk.Ado.Tests.Internal;

public class SqlParserTests
{
    [Fact]
    public void Parse_WhenDigitOrLetterOrUnderliningInParamName_ReturnParsedSqlAndCached()
    {
        var (sql, sqlParams) = SqlParser.Parse("SELECT @p; SELECT @p2; SELECT @p_3;");

        Assert.Equal("SELECT $p; SELECT $p2; SELECT $p_3;", sql);
        Assert.Equal(new PrimitiveParam[] { new("$p", false), new("$p2", false), new("$p_3", false) }, sqlParams);
    }

    [Fact]
    public void Parse_WhenNativeParameterInQuery_ReturnParsedSql()
    {
        var (sql, sqlParams) = SqlParser.Parse("DECLARE $p2 AS Text; SELECT @p; SELECT $p2; SELECT @p_3;");

        Assert.Equal("DECLARE $p2 AS Text; SELECT $p; SELECT $p2; SELECT $p_3;", sql);
        Assert.Equal(new PrimitiveParam[] { new("$p2", true), new("$p", false), new("$p_3", false) }, sqlParams);
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
        Assert.Equal(new PrimitiveParam[] { new("$param", false), new("$p2", false), new("$p_3", false) }, sqlParams);
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
            new("$name1", false), new("$name2", false), new("$name3", false),
            new("$name4", false), new("$name5", false), new("$name6", false),
            new("$name7", false), new("$name8", false), new("$name9", false),
            new("$name10", false), new("$name11", false), new("$name12", false),
            new("$name13", false), new("$name14", false), new("$name15", false),
            new("$name16", false), new("$name17", false), new("$name18", false),
            new("$name19", false), new("$name20", false)
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
        Assert.Equal(new PrimitiveParam[] { new("$air_date", false), new("$param", false) }, sqlParams);
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
        Assert.Equal(new PrimitiveParam[] { new("$air_date", false), new("$param", false) }, sqlParams);
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
        Assert.Equal(new PrimitiveParam[] { new("$air_date", false), new("$param", false) }, sqlParams);
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
        Assert.Equal(new PrimitiveParam[]
        {
            new("$text", true), new("$param", false),
            new("$p2", false), new("$p_3", false)
        }, sqlParams);
    }

    [Fact]
    public void Parse_WhenRepeatedOneParam_ReturnThisParamInParamNames()
    {
        var (sql, sqlParams) = SqlParser.Parse("SELECT @a, @a, @a;");

        Assert.Equal("SELECT $a, $a, $a;", sql);
        Assert.Equal(new PrimitiveParam[] { new("$a", false) }, sqlParams);
    }

    [Fact]
    public void Parse_WhenParamHasNextStatementIsNotWhitespace_ReturnSql()
    {
        var (sql, sqlParams) = SqlParser.Parse("SELECT @a/* this comment @ */");

        Assert.Equal("SELECT $a/* this comment @ */", sql);
        Assert.Equal(new PrimitiveParam[] { new("$a", false) }, sqlParams);
    }

    [Theory]
    [InlineData("SELECT a FROM Table WHERE b IN $Gen_List_Primitive_1", "SELECT a FROM Table WHERE b IN (@id1)", 1)]
    [InlineData("SELECT a FROM Table WHERE b IN $Gen_List_Primitive_1",
        "SELECT a FROM Table WHERE b IN (@id1, @id2, @id3, @id4, @id5)", 5)]
    [InlineData("SELECT a FROM Table WHERE b IN $Gen_List_Primitive_1",
        "SELECT a FROM Table WHERE b IN (/* comment in list parameters */ @id1, @id2, @id3, @id4)", 4)]
    [InlineData("SELECT a FROM Table WHERE b IN $Gen_List_Primitive_1;",
        "SELECT a FROM Table WHERE b IN (    @id1, @id2,   @id3, -- asdasdaksld\n @id4   );", 4)]
    [InlineData("SELECT a FROM Table WHERE b /* comment in list parameters */IN" +
                "/* comment in list parameters */$Gen_List_Primitive_1/* comment in list parameters */;",
        "SELECT a FROM Table WHERE b /* comment in list parameters */IN/* comment in list parameters */(" +
        "/* comment in list parameters */@id1,/* comment in list parameters */@id2," +
        "/* comment in list parameters */@id3/* comment in list parameters */)" +
        "/* comment in list parameters */;", 3)]
    [InlineData("SELECT a FROM Table WHERE b -- comment in list parameters \nIN" +
                "-- comment in list parameters \n$Gen_List_Primitive_1-- comment in list parameters \n;",
        "SELECT a FROM Table WHERE b -- comment in list parameters \nIN-- comment in list parameters \n(" +
        "-- comment in list parameters \n@id1,-- comment in list parameters \n@id2," +
        "-- comment in list parameters \n@id3-- comment in list parameters \n)" +
        "-- comment in list parameters \n;", 3)]
    public void Parse_WhenInListParameters_ReturnSqlWithListParam(string expectedSql, string actualSql, int listSize)
    {
        var (sql, sqlParams) = SqlParser.Parse(actualSql);
        Assert.Equal(expectedSql, sql);

        var ydbParameterCollection = new YdbParameterCollection();
        for (var i = 1; i <= listSize; i++)
        {
            ydbParameterCollection.AddWithValue($"$id{i}", i);
        }

        var ydbParameters = ydbParameterCollection.YdbParameters;
        var listYdbValues = sqlParams[0].YdbValueFetch(ydbParameters).Value.Items;

        Assert.Equal(listSize, listYdbValues.Count);
        for (var i = 1; i <= listSize; i++)
        {
            Assert.Equal(i, listYdbValues[i - 1].Int32Value);
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
    [InlineData("DECLARE $id1 AS Int32; SELECT a FROM Table WHERE b IN (@@asdasd@@,$id1,@@asdasd@@,$id2,@@asdasd@@)",
        "DECLARE $id1 AS Int32; SELECT a FROM Table WHERE b IN (@@asdasd@@,$id1,@@asdasd@@,$id2,@@asdasd@@)")]
    [InlineData("Select fun_in($ids1, $ids2)", "Select fun_in(@ids1, @ids2)")]
    [InlineData("Select in_fun($ids1, $ids2)", "Select in_fun(@ids1, @ids2)")]
    [InlineData("Select funin($ids1, $ids2)", "Select funin(@ids1, @ids2)")]
    [InlineData("Select infun($ids1, $ids2)", "Select infun(@ids1, @ids2)")]
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

        var ydbParameterCollection = new YdbParameterCollection();
        for (var i = 1; i <= 5; i++)
        {
            ydbParameterCollection.AddWithValue($"@id{i}", i);
        }

        ydbParameterCollection.AddWithValue("$simple_param_first", "first");
        ydbParameterCollection.AddWithValue("$simple_param_second", "second");

        var ydbParameters = ydbParameterCollection.YdbParameters;

        Assert.Equal("$Gen_List_Primitive_1", sqlParams[0].Name);
        var listPrimitive1 = sqlParams[0].YdbValueFetch(ydbParameters).Value.Items;
        Assert.Equal(5, listPrimitive1.Count);
        for (var i = 0; i < 5; i++)
        {
            Assert.Equal(i + 1, listPrimitive1[i].Int32Value);
        }

        Assert.Equal("$Gen_List_Primitive_2", sqlParams[2].Name);
        var listPrimitive2 = sqlParams[2].YdbValueFetch(ydbParameters).Value.Items;
        Assert.Equal(4, listPrimitive2.Count);
        for (var i = 0; i < 4; i++)
        {
            Assert.Equal(i + 1, listPrimitive2[i].Int32Value);
        }

        Assert.Equal("first", sqlParams[1].YdbValueFetch(ydbParameters).Value.TextValue);
        Assert.Equal("second", sqlParams[5].YdbValueFetch(ydbParameters).Value.TextValue);
        Assert.Equal(1, sqlParams[3].YdbValueFetch(ydbParameters).Value.Int32Value);
        Assert.Equal(2, sqlParams[4].YdbValueFetch(ydbParameters).Value.Int32Value);
    }
}
