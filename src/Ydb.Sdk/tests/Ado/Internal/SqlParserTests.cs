using Xunit;
using Ydb.Sdk.Ado.Internal;

namespace Ydb.Sdk.Tests.Ado.Internal;

public class SqlParserTests
{
    [Fact]
    public void Parse_WhenDigitOrLetterOrUnderliningInParamName_ReturnParsedSqlAndCached()
    {
        var (sql, paramNames) = SqlParser.Parse("SELECT @p; SELECT @p2; SELECT @p_3;");

        Assert.Equal("SELECT $p; SELECT $p2; SELECT $p_3;", sql);
        Assert.Equal(new[] { "$p", "$p2", "$p_3" }, paramNames);
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
        var (sql, paramNames) = SqlParser.Parse(@"-- Comment with params @param, @p2, @p_3
SELECT @param; SELECT @p2; SELECT @p_3;");

        Assert.Equal(@"-- Comment with params @param, @p2, @p_3
SELECT $param; SELECT $p2; SELECT $p_3;", sql);
        Assert.Equal(new[] { "$param", "$p2", "$p_3" }, paramNames);
    }

    [Fact]
    public void Parse_WhenBlockComment_ReturnSqlWithComment()
    {
        var (sql, paramNames) = SqlParser.Parse(@"/* Comment SQL
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
        Assert.Equal(new[]
        {
            "$name1", "$name2", "$name3", "$name4", "$name5", "$name6", "$name7", "$name8", "$name9", "$name10",
            "$name11", "$name12", "$name13", "$name14", "$name15", "$name16", "$name17", "$name18", "$name19", "$name20"
        }, paramNames);
    }

    [Fact]
    public void Parse_WhenDoubleQuotes_ReturnSql()
    {
        var (sql, paramNames) = SqlParser.Parse(@"REPLACE INTO episodes
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
        Assert.Equal(new[] { "$air_date", "$param" }, paramNames);
    }

    [Fact]
    public void Parse_WhenSingleQuotes_ReturnSql()
    {
        var (sql, paramNames) = SqlParser.Parse(@"UPSERT INTO episodes
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
        Assert.Equal(new[] { "$air_date", "$param" }, paramNames);
    }

    [Fact]
    public void Parse_WhenBacktickQuotes_ReturnSql()
    {
        var (sql, paramNames) = SqlParser.Parse(@"UPSERT INTO `episodes @ @ @ @  "" "" \` @ @ @`
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
        Assert.Equal(new[] { "$air_date", "$param" }, paramNames);
    }

    [Fact]
    public void Parse_WhenMultilineStringLiterals_ReturnSql()
    {
        var (sql, paramNames) = SqlParser.Parse(@"$text = @@some
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
        Assert.Equal(new[] { "$param", "$p2", "$p_3" }, paramNames);
    }

    [Fact]
    public void Parse_WhenRepeatedOneParam_ReturnThisParamInParamNames()
    {
        var (sql, paramNames) = SqlParser.Parse("SELECT @a, @a, @a;");

        Assert.Equal("SELECT $a, $a, $a;", sql);
        Assert.Equal(new[] { "$a" }, paramNames);
    }
}
