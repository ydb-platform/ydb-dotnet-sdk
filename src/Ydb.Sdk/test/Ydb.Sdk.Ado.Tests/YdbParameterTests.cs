using System.Data;
using System.Globalization;
using Xunit;
using Ydb.Sdk.Ado.YdbType;

namespace Ydb.Sdk.Ado.Tests;

public class YdbParameterTests : TestBase
{
    [Fact]
    public void YdbParameter_WhenValueIsNullAndDbTypeIsObject_ThrowException()
    {
        Assert.Equal("Writing value of 'null' is not supported without explicit mapping to the YdbDbType",
            Assert.Throws<InvalidOperationException>(() => new YdbParameter().TypedValue).Message);
        Assert.Equal("Writing value of 'System.Object' is not supported without explicit mapping to the YdbDbType",
            Assert.Throws<InvalidOperationException>(() => new YdbParameter("$param", new object()).TypedValue)
                .Message);
    }

    [Fact]
    public void YdbParameter_WhenSetWithoutAnyFormat_ReturnCorrectName()
    {
        Assert.Equal("$name", new YdbParameter { ParameterName = "name" }.ParameterName);
        Assert.Equal("$name", new YdbParameter { ParameterName = "@name" }.ParameterName);
        Assert.Equal("$name", new YdbParameter { ParameterName = "$name" }.ParameterName);
    }

    [Fact]
    public void YdbParameter_WhenUnCastTypes_ThrowInvalidCastException()
    {
        Assert.Equal("Writing value of 'System.Int32' is not supported for parameters having YdbDbType 'Bool'",
            Assert.Throws<InvalidOperationException>(() => new YdbParameter("$var", DbType.Boolean, 1).TypedValue)
                .Message);
        Assert.Equal("Writing value of 'System.Int32' is not supported for parameters having YdbDbType 'Int8'",
            Assert.Throws<InvalidOperationException>(() => new YdbParameter("$var", DbType.SByte, 1).TypedValue)
                .Message);
        Assert.Equal("Writing value of 'System.String' is not supported for parameters having YdbDbType 'Bool'",
            Assert.Throws<InvalidOperationException>(() => new YdbParameter("$parameter", DbType.Boolean)
                { Value = "true" }.TypedValue).Message);
        Assert.Equal("Writing value of 'System.Double' is not supported for parameters having YdbDbType 'Float'",
            Assert.Throws<InvalidOperationException>(() => new YdbParameter("$var", DbType.Single, 1.1).TypedValue)
                .Message);
    }

    [Theory]
    [InlineData(DbType.VarNumeric, "VarNumeric")]
    [InlineData(DbType.Xml, "Xml")]
    [InlineData(DbType.Time, "Time")]
    public void YdbParameter_WhenNoSupportedDbType_ThrowException(DbType dbType, string name) =>
        Assert.Equal("Ydb don't supported this DbType: " + name, Assert.Throws<NotSupportedException>(() =>
            new YdbParameter("$parameter", dbType) { IsNullable = true }.TypedValue).Message);

    [Fact]
    public void YdbParameter_WhenSetAndNoSet_ReturnValueOrException()
    {
        Assert.Equal("$parameter", new YdbParameter { ParameterName = "$parameter" }.ParameterName);
        Assert.Equal(string.Empty, new YdbParameter { ParameterName = null }.ParameterName);
    }

    [Fact]
    public void YdbParameter_WhenSetDbType_ReturnValueIsConverted()
    {
        Assert.Equal(1U, new YdbParameter("$parameter", DbType.UInt64) { Value = 1U }.TypedValue.Value.Uint64Value);
        Assert.Equal(1U,
            new YdbParameter("$parameter", DbType.UInt64) { Value = (ushort)1U }.TypedValue.Value.Uint64Value);
        Assert.Equal(1U,
            new YdbParameter("$parameter", DbType.UInt64) { Value = (byte)1U }.TypedValue.Value.Uint64Value);

        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int64) { Value = 1 }.TypedValue.Value.Int64Value);
        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int64) { Value = (uint)1 }.TypedValue.Value.Int64Value);
        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int64) { Value = (ushort)1 }.TypedValue.Value.Int64Value);
        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int64) { Value = (byte)1 }.TypedValue.Value.Int64Value);
        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int64) { Value = (short)1 }.TypedValue.Value.Int64Value);
        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int64) { Value = (sbyte)1 }.TypedValue.Value.Int64Value);

        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int32) { Value = (ushort)1 }.TypedValue.Value.Int32Value);
        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int32) { Value = (byte)1 }.TypedValue.Value.Int32Value);
        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int32) { Value = (short)1 }.TypedValue.Value.Int32Value);
        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int32) { Value = (sbyte)1 }.TypedValue.Value.Int32Value);

        Assert.Equal(1U,
            new YdbParameter("$parameter", DbType.UInt32) { Value = (ushort)1 }.TypedValue.Value.Uint32Value);
        Assert.Equal(1U,
            new YdbParameter("$parameter", DbType.UInt32) { Value = (byte)1 }.TypedValue.Value.Uint32Value);

        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int16) { Value = (byte)1 }.TypedValue.Value.Int32Value);
        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int16) { Value = (sbyte)1 }.TypedValue.Value.Int32Value);

        Assert.Equal(1U,
            new YdbParameter("$parameter", DbType.UInt16) { Value = (byte)1 }.TypedValue.Value.Uint32Value);

        Assert.Equal(1.1f, new YdbParameter("$parameter", DbType.Double) { Value = 1.1f }.TypedValue.Value.DoubleValue);
    }

    [Fact]
    public async Task YdbValue_WhenDateTimeOffset_ReturnTimestamp()
    {
        var dateTimeOffset = DateTimeOffset.Parse("2029-08-03T06:59:44.8578730Z");
        await using var ydbConnection = await CreateOpenConnectionAsync();
        var tableTableName = $"dateTimeOffset_{Random.Shared.Next()}";
        await new YdbCommand(ydbConnection)
            {
                CommandText = $"CREATE TABLE {tableTableName} (TimestampField Timestamp, PRIMARY KEY (TimestampField))"
            }
            .ExecuteNonQueryAsync();
        await new YdbCommand(ydbConnection)
            {
                CommandText =
                    $"INSERT INTO {tableTableName}(TimestampField) " +
                    $"VALUES (@parameter1), (@parameter2), (@parameter3), (@parameter4)",
                Parameters =
                {
                    new YdbParameter("$parameter1", dateTimeOffset),
                    new YdbParameter("$parameter2", DbType.DateTimeOffset, dateTimeOffset.AddHours(1)),
                    new YdbParameter("$parameter3", DbType.DateTimeOffset, dateTimeOffset.AddHours(2)),
                    new YdbParameter("$parameter4", YdbDbType.Timestamp, dateTimeOffset.AddHours(3))
                }
            }
            .ExecuteNonQueryAsync();

        var ydbDataReader = await new YdbCommand(ydbConnection) { CommandText = $"SELECT * FROM {tableTableName}" }
            .ExecuteReaderAsync();

        var hourCount = 0;
        while (ydbDataReader.NextResult())
        {
            Assert.True(ydbDataReader.Read());
            Assert.Equal(dateTimeOffset.AddHours(hourCount++).UtcDateTime, ydbDataReader.GetValue(0));
        }

        await new YdbCommand(ydbConnection) { CommandText = $"DROP TABLE {tableTableName};" }.ExecuteNonQueryAsync();
    }

    [Theory]
    [InlineData("123e4567-e89b-12d3-a456-426614174000")]
    [InlineData("2d9e498b-b746-9cfb-084d-de4e1cb4736e")]
    [InlineData("6E73B41C-4EDE-4D08-9CFB-B7462D9E498B")]
    public async Task Guid_WhenSelectUuid_ReturnThisUuid(string guid)
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        var actualGuid = await new YdbCommand(ydbConnection)
                { CommandText = $"SELECT CAST('{guid}' AS UUID);" }
            .ExecuteScalarAsync();

        Assert.Equal(new Guid(guid), actualGuid);
        Assert.Equal(guid.ToLower(), actualGuid?.ToString()); // Guid.ToString() method represents lowercase
    }

    [Theory]
    [InlineData("123e4567-e89b-12d3-a456-426614174000")]
    [InlineData("2d9e498b-b746-9cfb-084d-de4e1cb4736e")]
    [InlineData("6E73B41C-4EDE-4D08-9CFB-B7462D9E498B")]
    public async Task Guid_WhenSetUuid_ReturnThisUtf8Uuid(string guid)
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        var ydbCommand = new YdbCommand(ydbConnection)
        {
            CommandText = "SELECT CAST(@guid AS Text);"
        };
        ydbCommand.Parameters.Add(new YdbParameter("guid", DbType.Guid, new Guid(guid)));

        var actualGuidText = await ydbCommand.ExecuteScalarAsync();

        Assert.Equal(guid.ToLower(), actualGuidText); // Guid.ToString() method represents lowercase
    }

    [Fact]
    public async Task Date_WhenSetDateOnly_ReturnDateTime()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        var ydbCommand = new YdbCommand(ydbConnection) { CommandText = "SELECT @dateOnly;" };
        ydbCommand.Parameters.AddWithValue("dateOnly", new DateOnly(2002, 2, 24));

        Assert.Equal(new DateTime(2002, 2, 24), await ydbCommand.ExecuteScalarAsync());

        ydbCommand.Parameters.Clear();
        ydbCommand.Parameters.AddWithValue("dateOnly", DbType.Date, new DateOnly(2102, 2, 24));
        Assert.Equal(new DateTime(2102, 2, 24), await ydbCommand.ExecuteScalarAsync());
    }

    [Theory]
    [InlineData("12345", "12345.0000000000", 22, 9)]
    [InlineData("54321", "54321", 5, 0)]
    [InlineData("493235.4", "493235.40", 7, 2)]
    [InlineData("123.46", "123.46", 5, 2)]
    [InlineData("-184467434073.70911616", "-184467434073.7091161600", 35, 10)]
    [InlineData("-18446744074", "-18446744074", 12, 0)]
    [InlineData("-184467440730709551616", "-184467440730709551616", 21, 0)]
    [InlineData("-218446744073.709551616", "-218446744073.7095516160", 22, 10)]
    [InlineData(null, null, 22, 9)]
    [InlineData(null, null, 35, 9)]
    [InlineData(null, null, 35, 0)]
    public async Task Decimal_WhenDecimalIsScaleAndPrecision_ReturnDecimal(string? value, string? expected,
        byte precision, byte scale)
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        var decimalTableName = $"DecimalTable_{Random.Shared.Next()}";
        var decimalValue = value == null ? (decimal?)null : decimal.Parse(value, CultureInfo.InvariantCulture);
        await new YdbCommand(ydbConnection)
        {
            CommandText = $"""
                           CREATE TABLE {decimalTableName} (
                                DecimalField Decimal({precision}, {scale}),
                                PRIMARY KEY (DecimalField)
                           )
                           """
        }.ExecuteNonQueryAsync();
        await new YdbCommand(ydbConnection)
        {
            CommandText = $"INSERT INTO {decimalTableName}(DecimalField) VALUES (@DecimalField);",
            Parameters =
            {
                new YdbParameter("DecimalField", DbType.Decimal, decimalValue) { Precision = precision, Scale = scale }
            }
        }.ExecuteNonQueryAsync();

        Assert.Equal(expected == null ? DBNull.Value : decimal.Parse(expected, CultureInfo.InvariantCulture),
            await new YdbCommand(ydbConnection) { CommandText = $"SELECT DecimalField FROM {decimalTableName};" }
                .ExecuteScalarAsync());

        await new YdbCommand(ydbConnection) { CommandText = $"DROP TABLE {decimalTableName};" }.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task YdbParameter_WhenYdbDbTypeSetAndValueIsNull_ReturnsNullValue()
    {
        foreach (var ydbType in Enum.GetValues<YdbDbType>())
        {
            if (ydbType == YdbDbType.Unspecified) continue;

            var tableName = $"YdbDbType_{Random.Shared.Next()}";
            await using var ydbConnection = await CreateOpenConnectionAsync();
            var ydbTypeStr = ydbType == YdbDbType.Decimal ? "Decimal(22, 9)" : ydbType.ToString();
            await new YdbCommand(ydbConnection)
                    { CommandText = $"CREATE TABLE {tableName}(Id Int32, Type {ydbTypeStr}, PRIMARY KEY (Id))" }
                .ExecuteNonQueryAsync();

            await new YdbCommand(ydbConnection)
            {
                CommandText = $"INSERT INTO {tableName}(Id, Type) VALUES (1, @Type);",
                Parameters = { new YdbParameter("Type", ydbType) }
            }.ExecuteNonQueryAsync();

            Assert.Equal(DBNull.Value, await new YdbCommand(ydbConnection)
            {
                CommandText = $"SELECT Type FROM {tableName} WHERE Id = @Id;",
                Parameters = { new YdbParameter("Id", DbType.Int32, 1) }
            }.ExecuteScalarAsync());

            await new YdbCommand(ydbConnection) { CommandText = $"DROP TABLE {tableName};" }.ExecuteNonQueryAsync();
        }
    }
}
