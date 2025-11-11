using System.Collections;
using System.Data;
using System.Globalization;
using Xunit;
using Ydb.Sdk.Ado.YdbType;
using Ydb.Sdk.Value;

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
    public void YdbValue_WhenYdbValueIsSet_ReturnThis() =>
        Assert.Equal("{\"type\": \"jsondoc\"}", new YdbParameter("$parameter",
            YdbValue.MakeJsonDocument("{\"type\": \"jsondoc\"}")).TypedValue.Value.TextValue);

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
        var ydbDataReader = await ydbCommand.ExecuteReaderAsync();
        await ydbDataReader.ReadAsync();
        Assert.Equal(new DateOnly(2002, 2, 24), ydbDataReader.GetFieldValue<DateOnly>(0));
        Assert.False(await ydbDataReader.ReadAsync());

        ydbCommand.Parameters.Clear();
        ydbCommand.Parameters.AddWithValue("dateOnly", DbType.Date, new DateOnly(2102, 2, 24));
        Assert.Equal(new DateTime(2102, 2, 24), await ydbCommand.ExecuteScalarAsync());

        ydbDataReader = await ydbCommand.ExecuteReaderAsync();
        await ydbDataReader.ReadAsync();
        Assert.Equal(new DateOnly(2102, 2, 24), ydbDataReader.GetFieldValue<DateOnly>(0));
        Assert.False(await ydbDataReader.ReadAsync());
    }

    [Theory]
    [InlineData("12345", "12345.0000000000", 22, 9)]
    [InlineData("54321", "54321", 5, 0)]
    [InlineData("493235.4", "493235.40", 8, 2)]
    [InlineData("123.46", "123.46", 5, 2)]
    [InlineData("0.46", "0.46", 2, 2)]
    [InlineData("-184467434073.70911616", "-184467434073.7091161600", 35, 10)]
    [InlineData("-18446744074", "-18446744074", 12, 0)]
    [InlineData("-184467440730709551616", "-184467440730709551616", 21, 0)]
    [InlineData("-218446744073.709551616", "-218446744073.7095516160", 22, 10)]
    [InlineData("79228162514264337593543950335", "79228162514264337593543950335", 29, 0)]
    [InlineData("79228162514264337593543950.335", "79228162514264337593543950.335", 29, 3)]
    [InlineData("-79228162514264337593543950335", "-79228162514264337593543950335", 29, 0)]
    [InlineData("-79228162514264337593543950.335", "-79228162514264337593543950.335", 29, 3)]
    [InlineData(null, null, 22, 9)]
    [InlineData(null, null, 35, 9)]
    [InlineData(null, null, 35, 0)]
    public async Task Decimal_WhenDecimalIsScaleAndPrecision_ReturnDecimal(string? value, string? expected,
        byte precision, byte scale)
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        var tableName = $"DecimalTable_{Random.Shared.Next()}";
        var decimalValue = value == null ? (decimal?)null : decimal.Parse(value, CultureInfo.InvariantCulture);
        await new YdbCommand(ydbConnection)
                { CommandText = $"CREATE TABLE {tableName} (d Decimal({precision}, {scale}), PRIMARY KEY (d))" }
            .ExecuteNonQueryAsync();
        await new YdbCommand(ydbConnection)
        {
            CommandText = $"INSERT INTO {tableName}(d) VALUES (@d);",
            Parameters =
                { new YdbParameter("d", DbType.Decimal, decimalValue) { Precision = precision, Scale = scale } }
        }.ExecuteNonQueryAsync();

        Assert.Equal(expected == null ? DBNull.Value : decimal.Parse(expected, CultureInfo.InvariantCulture),
            await new YdbCommand(ydbConnection) { CommandText = $"SELECT d FROM {tableName};" }.ExecuteScalarAsync());

        // IN (NULL) returns empty result set
        Assert.Equal(expected == null ? null : decimal.Parse(expected, CultureInfo.InvariantCulture),
            await new YdbCommand(ydbConnection)
            {
                CommandText = $"SELECT d FROM {tableName} WHERE d IN @d;",
                Parameters = { new YdbParameter("d", new[] { decimalValue }) { Precision = precision, Scale = scale } }
            }.ExecuteScalarAsync());

        // IN NULL always returns an empty result set
        Assert.Null(await new YdbCommand(ydbConnection)
        {
            CommandText = $"SELECT d FROM {tableName} WHERE d IN @d;",
            Parameters =
            {
                new YdbParameter("d", YdbDbType.List | YdbDbType.Decimal)
                    { Precision = precision, Scale = scale }
            }
        }.ExecuteScalarAsync());

        await new YdbCommand(ydbConnection) { CommandText = $"DROP TABLE {tableName};" }.ExecuteNonQueryAsync();
    }

    [Theory]
    [InlineData("123.456", 5, 2)]
    [InlineData("1.46", 2, 2)]
    [InlineData("654321", 5, 0)]
    [InlineData("493235.4", 7, 2)]
    [InlineData("10.46", 3, 2)]
    [InlineData("99.999", 5, 2)]
    [InlineData("0.001", 3, 2)]
    [InlineData("-12.345", 5, 2)]
    [InlineData("7.001", 4, 2)]
    [InlineData("1.0001", 5, 3)]
    [InlineData("1000.00", 5, 2)]
    [InlineData("123456.7", 6, 1)]
    [InlineData("999.99", 5, 4)]
    [InlineData("-100", 2, 0)]
    [InlineData("-0.12", 2, 1)]
    [InlineData("10.0", 2, 0)]
    [InlineData("-0.1", 1, 0)]
    [InlineData("10000", 4, 0)]
    [InlineData("12345", 4, 0)]
    [InlineData("12.3456", 6, 3)]
    [InlineData("123.45", 4, 1)]
    [InlineData("9999.9", 5, 0)]
    [InlineData("-1234.56", 5, 1)]
    [InlineData("-1000", 3, 0)]
    [InlineData("0.0001", 4, 3)]
    [InlineData("99999", 4, 0)]
    [InlineData("9.999", 3, 2)]
    [InlineData("123.4", 3, 0)]
    [InlineData("1.234", 4, 2)]
    [InlineData("-98.765", 5, 2)]
    [InlineData("100.01", 5, 1)]
    [InlineData("100000", 5, 0)]
    [InlineData("12345678901", 10, 0)]
    public async Task Decimal_WhenNotRepresentableBySystemDecimal_ThrowsOverflowException(string value, byte precision,
        byte scale)
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        var tableName = $"DecimalOverflowTable__{Random.Shared.Next()}";
        var decimalValue = decimal.Parse(value, CultureInfo.InvariantCulture);
        await new YdbCommand(ydbConnection)
                { CommandText = $"CREATE TABLE {tableName}(d Decimal({precision}, {scale}), PRIMARY KEY(d))" }
            .ExecuteNonQueryAsync();

        Assert.Equal($"Value {decimalValue} does not fit Decimal({precision}, {scale})",
            (await Assert.ThrowsAsync<OverflowException>(() => new YdbCommand(ydbConnection)
            {
                CommandText = $"INSERT INTO {tableName}(d) VALUES (@d);",
                Parameters =
                {
                    new YdbParameter("d", DbType.Decimal, decimal.Parse(value, CultureInfo.InvariantCulture))
                        { Value = decimalValue, Precision = precision, Scale = scale }
                }
            }.ExecuteNonQueryAsync())).Message);

        Assert.Equal(0ul, (ulong)(await new YdbCommand(ydbConnection)
            { CommandText = $"SELECT COUNT(*) FROM {tableName};" }.ExecuteScalarAsync())!);

        await new YdbCommand(ydbConnection) { CommandText = $"DROP TABLE {tableName};" }.ExecuteNonQueryAsync();
    }


    [Fact]
    public void Decimal_WhenScaleGreaterThanPrecision_ThrowsArgumentOutOfRangeException() =>
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new YdbParameter("d", DbType.Decimal, 0.0m) { Precision = 1, Scale = 2 }.TypedValue);

    [Theory]
    [InlineData("10000000000000000000000000000000000", 35, 0)]
    [InlineData("1000000000000000000000000.0000000000", 35, 10)]
    [InlineData("1000000000000000000000000000000000", 34, 0)]
    [InlineData("100000000000000000000000.0000000000", 34, 10)]
    [InlineData("100000000000000000000000000000000", 33, 0)]
    [InlineData("10000000000000000000000.0000000000", 33, 10)]
    [InlineData("-10000000000000000000000000000000", 32, 0)]
    [InlineData("-1000000000000000000000.0000000000", 32, 10)]
    [InlineData("-1000000000000000000000000000000", 31, 0)]
    [InlineData("-100000000000000000000.0000000000", 31, 10)]
    [InlineData("1000000000000000000000000000000", 30, 0)]
    [InlineData("100000000000000000000.0000000000", 30, 10)]
    [InlineData("79228162514264337593543950336", 29, 0)]
    [InlineData("79228162514264337593543950.336", 29, 3)]
    [InlineData("-79228162514264337593543950336", 29, 0)]
    [InlineData("-79228162514264337593543950.336", 29, 3)]
    [InlineData("100000", 4, 0)] // inf
    public async Task Decimal_WhenYdbReturnsDecimalWithPrecisionGreaterThan28_ThrowsOverflowException(string value,
        int precision, int scale)
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        Assert.Equal("Value does not fit into decimal", (await Assert.ThrowsAsync<OverflowException>(() =>
            new YdbCommand(ydbConnection)
                    { CommandText = $"SELECT (CAST('{value}' AS Decimal({precision}, {scale})));" }
                .ExecuteScalarAsync())).Message);
    }

    [Fact]
    public async Task YdbParameter_WhenYdbDbTypeSetAndValueIsNull_ReturnsNullValue()
    {
        foreach (var ydbDbType in Enum.GetValues<YdbDbType>())
        {
            if (ydbDbType is YdbDbType.Unspecified or YdbDbType.List) continue;

            var tableName = $"Null_YdbDbType_{Random.Shared.Next()}";
            await using var ydbConnection = await CreateOpenConnectionAsync();
            var ydbTypeStr = ydbDbType == YdbDbType.Decimal ? "Decimal(22, 9)" : ydbDbType.ToString();
            await new YdbCommand(ydbConnection)
                    { CommandText = $"CREATE TABLE {tableName}(Id Int32, Type {ydbTypeStr}, PRIMARY KEY (Id))" }
                .ExecuteNonQueryAsync();

            await new YdbCommand(ydbConnection)
            {
                CommandText = $"INSERT INTO {tableName}(Id, Type) VALUES (1, @Type);",
                Parameters = { new YdbParameter("Type", ydbDbType) }
            }.ExecuteNonQueryAsync();

            Assert.Equal(DBNull.Value, await new YdbCommand(ydbConnection)
            {
                CommandText = $"SELECT Type FROM {tableName} WHERE Id = @Id;",
                Parameters = { new YdbParameter("Id", DbType.Int32, 1) }
            }.ExecuteScalarAsync());

            // Error: Can't lookup Optional<T> in collection of JsonDocument: types Optional<T> and T are not comparable
            if (ydbDbType is not (YdbDbType.Yson or YdbDbType.Json or YdbDbType.JsonDocument))
                Assert.Null(await new YdbCommand(ydbConnection)
                {
                    CommandText = $"SELECT Type FROM {tableName} WHERE Type IN @Type;",
                    Parameters = { new YdbParameter("Type", YdbDbType.List | ydbDbType) }
                }.ExecuteScalarAsync()); // return empty

            await new YdbCommand(ydbConnection) { CommandText = $"DROP TABLE {tableName};" }.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task YdbParameter_WhenYdbDbTypeSetAndValueIsNotNull_ReturnsValue()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        var tableName = $"NonNull_YdbDbType_{Random.Shared.Next()}";
        await new YdbCommand(ydbConnection)
        {
            CommandText = $"""
                           CREATE TABLE {tableName} (
                               Int32Column Int32,
                               BoolColumn Bool NOT NULL,
                               Int64Column Int64 NOT NULL,
                               Int16Column Int16 NOT NULL,
                               Int8Column Int8 NOT NULL,
                               FloatColumn Float NOT NULL,
                               DoubleColumn Double NOT NULL,
                               DefaultDecimalColumn Decimal(22, 9) NOT NULL,
                               CustomDecimalColumn Decimal(35, 5) NOT NULL,
                               Uint8Column Uint8 NOT NULL,
                               Uint16Column Uint16 NOT NULL,
                               Uint32Column Uint32 NOT NULL,
                               Uint64Column Uint64 NOT NULL,
                               TextColumn Text NOT NULL,
                               BytesColumn Bytes NOT NULL,
                               DateColumn Date NOT NULL,
                               DatetimeColumn Datetime NOT NULL,
                               TimestampColumn Timestamp NOT NULL,
                               IntervalColumn Interval NOT NULL,
                               JsonColumn Json NOT NULL,
                               JsonDocumentColumn JsonDocument NOT NULL,
                               YsonColumn Yson NOT NULL,
                               Date32Column Date32 NOT NULL,
                               Datetime64Column DateTime64 NOT NULL,
                               Timestamp64Column Timestamp64 NOT NULL,
                               Interval64Column Interval64 NOT NULL,
                               PRIMARY KEY (Int32Column)
                           );
                           """
        }.ExecuteNonQueryAsync();

        await new YdbCommand(ydbConnection)
        {
            CommandText = $"""
                           INSERT INTO {tableName} (
                               Int32Column, BoolColumn, Int64Column, Int16Column, Int8Column, FloatColumn, DoubleColumn, 
                               DefaultDecimalColumn, CustomDecimalColumn, Uint8Column, Uint16Column, Uint32Column, 
                               Uint64Column, TextColumn, BytesColumn, DateColumn, DatetimeColumn, TimestampColumn,
                               IntervalColumn, JsonColumn, JsonDocumentColumn, YsonColumn, Date32Column, Datetime64Column,
                               Timestamp64Column,  Interval64Column
                           ) VALUES (
                               @Int32Column, @BoolColumn, @Int64Column, @Int16Column, @Int8Column, @FloatColumn, 
                               @DoubleColumn, @DefaultDecimalColumn, @CustomDecimalColumn, @Uint8Column, @Uint16Column, 
                               @Uint32Column, @Uint64Column, @TextColumn, @BytesColumn, @DateColumn, @DatetimeColumn, 
                               @TimestampColumn, @IntervalColumn, @JsonColumn, @JsonDocumentColumn, @YsonColumn, @Date32Column,
                               @Datetime64Column,  @Timestamp64Column, @Interval64Column
                           );
                           """,
            Parameters =
            {
                new YdbParameter("Int32Column", YdbDbType.Int32, 1),
                new YdbParameter("BoolColumn", YdbDbType.Bool, true),
                new YdbParameter("Int64Column", YdbDbType.Int64, 1),
                new YdbParameter("Int16Column", YdbDbType.Int16, (short)1),
                new YdbParameter("Int8Column", YdbDbType.Int8, (sbyte)1),
                new YdbParameter("FloatColumn", YdbDbType.Float, 1.0f),
                new YdbParameter("DoubleColumn", YdbDbType.Double, 1.0),
                new YdbParameter("DefaultDecimalColumn", YdbDbType.Decimal, 1m),
                new YdbParameter("CustomDecimalColumn", YdbDbType.Decimal, 1m) { Precision = 35, Scale = 5 },
                new YdbParameter("Uint8Column", YdbDbType.Uint8, (byte)1),
                new YdbParameter("Uint16Column", YdbDbType.Uint16, (ushort)1),
                new YdbParameter("Uint32Column", YdbDbType.Uint32, (uint)1),
                new YdbParameter("Uint64Column", YdbDbType.Uint64, (ulong)1),
                new YdbParameter("TextColumn", YdbDbType.Text, string.Empty),
                new YdbParameter("BytesColumn", YdbDbType.Bytes, Array.Empty<byte>()),
                new YdbParameter("DateColumn", YdbDbType.Date, DateTime.UnixEpoch),
                new YdbParameter("DatetimeColumn", YdbDbType.Datetime, DateTime.UnixEpoch),
                new YdbParameter("TimestampColumn", YdbDbType.Timestamp, DateTime.UnixEpoch),
                new YdbParameter("IntervalColumn", YdbDbType.Interval, TimeSpan.Zero),
                new YdbParameter("JsonColumn", YdbDbType.Json, "{}"),
                new YdbParameter("JsonDocumentColumn", YdbDbType.JsonDocument, "{}"),
                new YdbParameter("YsonColumn", YdbDbType.Yson, "{a=1u}"u8.ToArray()),
                new YdbParameter("Date32Column", YdbDbType.Date32, DateTime.MinValue),
                new YdbParameter("Datetime64Column", YdbDbType.Datetime64, DateTime.MinValue),
                new YdbParameter("Timestamp64Column", YdbDbType.Timestamp64, DateTime.MinValue),
                new YdbParameter("Interval64Column", YdbDbType.Interval64,
                    TimeSpan.FromMilliseconds(TimeSpan.MinValue.Milliseconds))
            }
        }.ExecuteNonQueryAsync();

        var ydbDataReader = await new YdbCommand(ydbConnection)
        {
            CommandText = $"""
                           SELECT 
                               Int32Column, BoolColumn, Int64Column, Int16Column, Int8Column, FloatColumn, DoubleColumn, 
                               DefaultDecimalColumn, CustomDecimalColumn, Uint8Column, Uint16Column, Uint32Column, 
                               Uint64Column, TextColumn, BytesColumn, DateColumn, DatetimeColumn, TimestampColumn,
                               IntervalColumn, JsonColumn, JsonDocumentColumn, YsonColumn, Date32Column, Datetime64Column,  
                               Timestamp64Column, Interval64Column
                           FROM {tableName};
                           """
        }.ExecuteReaderAsync();

        Assert.True(ydbDataReader.Read());
        Assert.Equal(1, ydbDataReader.GetInt32(0));
        Assert.True(ydbDataReader.GetBoolean(1));
        Assert.Equal(1, ydbDataReader.GetInt64(2));
        Assert.Equal(1, ydbDataReader.GetInt16(3));
        Assert.Equal(1, ydbDataReader.GetSByte(4));
        Assert.Equal(1.0, ydbDataReader.GetFloat(5));
        Assert.Equal(1.0, ydbDataReader.GetDouble(6));
        Assert.Equal(1.000000000m, ydbDataReader.GetDecimal(7));
        Assert.Equal(1.00000m, ydbDataReader.GetDecimal(8));
        Assert.Equal(1, ydbDataReader.GetByte(9));
        Assert.Equal(1, ydbDataReader.GetUint16(10));
        Assert.Equal((uint)1, ydbDataReader.GetUint32(11));
        Assert.Equal((ulong)1, ydbDataReader.GetUint64(12));
        Assert.Equal(string.Empty, ydbDataReader.GetString(13));
        Assert.Equal(Array.Empty<byte>(), ydbDataReader.GetBytes(14));
        Assert.Equal(DateTime.UnixEpoch, ydbDataReader.GetDateTime(15));
        Assert.Equal(DateTime.UnixEpoch, ydbDataReader.GetDateTime(16));
        Assert.Equal(DateTime.UnixEpoch, ydbDataReader.GetDateTime(17));
        Assert.Equal(TimeSpan.Zero, ydbDataReader.GetInterval(18));
        Assert.Equal("{}", ydbDataReader.GetString(19));
        Assert.Equal("{}", ydbDataReader.GetString(20));
        Assert.Equal("{a=1u}"u8.ToArray(), ydbDataReader.GetBytes(21));
        Assert.Equal(DateTime.MinValue, ydbDataReader.GetDateTime(22));
        Assert.Equal(DateTime.MinValue, ydbDataReader.GetDateTime(23));
        Assert.Equal(DateTime.MinValue, ydbDataReader.GetDateTime(24));
        Assert.Equal(TimeSpan.FromMilliseconds(TimeSpan.MinValue.Milliseconds), ydbDataReader.GetInterval(25));
        Assert.False(ydbDataReader.Read());
        await ydbDataReader.CloseAsync();

        await new YdbCommand(ydbConnection) { CommandText = $"DROP TABLE {tableName}" }.ExecuteNonQueryAsync();
    }

    private static readonly DateTime SomeTimestamp = DateTime.Parse("2025-11-02T18:47:14.112353");
    private static readonly DateTime SomeDatetime = DateTime.Parse("2025-11-02T18:47");
    private static readonly DateTime SomeDate = DateTime.Parse("2025-11-02");

    public static TheoryData<YdbDbType, IList> ListParams => new()
    {
        { YdbDbType.Bool, new List<bool> { false, true, false } },
        { YdbDbType.Bool, (bool[])[false, true, false] },
        { YdbDbType.Int8, new List<sbyte> { 1, 2, 3 } },
        { YdbDbType.Int8, new sbyte[] { 1, 2, 3 } },
        { YdbDbType.Int16, new List<short> { 1, 2, 3 } },
        { YdbDbType.Int16, new short[] { 1, 2, 3 } },
        { YdbDbType.Int32, new List<int> { 1, 2, 3 } },
        { YdbDbType.Int32, (int[])[1, 2, 3] },
        { YdbDbType.Int64, new List<long> { 1, 2, 3 } },
        { YdbDbType.Int64, new long[] { 1, 2, 3 } },
        { YdbDbType.Uint8, new List<byte> { 1, 2, 3 } },
        { YdbDbType.Uint16, new List<ushort> { 1, 2, 3 } },
        { YdbDbType.Uint16, new ushort[] { 1, 2, 3 } },
        { YdbDbType.Uint32, new List<uint> { 1, 2, 3 } },
        { YdbDbType.Uint32, new uint[] { 1, 2, 3 } },
        { YdbDbType.Uint64, new List<ulong> { 1, 2, 3 } },
        { YdbDbType.Uint64, new ulong[] { 1, 2, 3 } },
        { YdbDbType.Float, new List<float> { 1, 2, 3 } },
        { YdbDbType.Float, new float[] { 1, 2, 3 } },
        { YdbDbType.Double, new List<double> { 1, 2, 3 } },
        { YdbDbType.Double, new double[] { 1, 2, 3 } },
        { YdbDbType.Decimal, new List<decimal> { 1, 2, 3 } },
        { YdbDbType.Decimal, new decimal[] { 1, 2, 3 } },
        { YdbDbType.Text, new List<string> { "1", "2", "3" } },
        { YdbDbType.Text, (string[])["1", "2", "3"] },
        { YdbDbType.Bytes, new List<byte[]> { new byte[] { 1, 1 }, new byte[] { 2, 2 }, new byte[] { 3, 3 } } },
        { YdbDbType.Bytes, (byte[][])[[1, 1], [2, 2], [3, 3]] },
        { YdbDbType.Date, new List<DateOnly> { new(2001, 2, 26), new(2002, 2, 24), new(2010, 3, 14) } },
        { YdbDbType.Date, new DateOnly[] { new(2001, 2, 26), new(2002, 2, 24), new(2010, 3, 14) } },
        {
            YdbDbType.Timestamp,
            new List<DateTime> { SomeTimestamp.AddDays(1), SomeTimestamp.AddDays(2), SomeTimestamp.AddDays(3) }
        },
        {
            YdbDbType.Timestamp,
            (DateTime[])[SomeTimestamp.AddDays(1), SomeTimestamp.AddDays(2), SomeTimestamp.AddDays(3)]
        },
        { YdbDbType.Interval, new List<TimeSpan> { TimeSpan.FromDays(1), TimeSpan.FromDays(2), TimeSpan.FromDays(3) } },
        { YdbDbType.Interval, (TimeSpan[])[TimeSpan.FromDays(1), TimeSpan.FromDays(2), TimeSpan.FromDays(3)] },
        { YdbDbType.Bool, new List<bool?> { false, true, false, null } },
        { YdbDbType.Bool, (bool?[])[false, true, false, null] },
        { YdbDbType.Int8, new List<sbyte?> { 1, 2, 3, null } },
        { YdbDbType.Int8, new sbyte?[] { 1, 2, 3, null } },
        { YdbDbType.Int16, new List<short?> { 1, 2, 3, null } },
        { YdbDbType.Int16, new short?[] { 1, 2, 3, null } },
        { YdbDbType.Int32, new List<int?> { 1, 2, 3, null } },
        { YdbDbType.Int32, (int?[])[1, 2, 3, null] },
        { YdbDbType.Int64, new List<long?> { 1, 2, 3, null } },
        { YdbDbType.Int64, new long?[] { 1, 2, 3, null } },
        { YdbDbType.Uint8, new List<byte?> { 1, 2, 3, null } },
        { YdbDbType.Uint16, new List<ushort?> { 1, 2, 3, null } },
        { YdbDbType.Uint16, new ushort?[] { 1, 2, 3, null } },
        { YdbDbType.Uint32, new List<uint?> { 1, 2, 3, null } },
        { YdbDbType.Uint32, new uint?[] { 1, 2, 3, null } },
        { YdbDbType.Uint64, new List<ulong?> { 1, 2, 3, null } },
        { YdbDbType.Uint64, new ulong?[] { 1, 2, 3, null } },
        { YdbDbType.Float, new List<float?> { 1, 2, 3, null } },
        { YdbDbType.Float, new float?[] { 1, 2, 3, null } },
        { YdbDbType.Double, new List<double?> { 1, 2, 3, null } },
        { YdbDbType.Double, new double?[] { 1, 2, 3, null } },
        { YdbDbType.Decimal, new List<decimal?> { 1, 2, 3, null } },
        { YdbDbType.Decimal, new decimal?[] { 1, 2, 3, null } },
        { YdbDbType.Text, new List<string?> { "1", "2", "3", null } },
        { YdbDbType.Text, (string?[])["1", "2", "3", null] },
        { YdbDbType.Bytes, new List<byte[]?> { new byte[] { 1, 1 }, new byte[] { 2, 2 }, new byte[] { 3, 3 }, null } },
        { YdbDbType.Bytes, (byte[]?[])[[1, 1], [2, 2], [3, 3], null] },
        {
            YdbDbType.Date, new List<DateOnly?>
                { new DateOnly(2001, 2, 26), new DateOnly(2002, 2, 24), new DateOnly(2010, 3, 14), null }
        },
        { YdbDbType.Date, new DateOnly?[] { new(2001, 2, 26), new(2002, 2, 24), new(2010, 3, 14), null } },
        {
            YdbDbType.Timestamp,
            new List<DateTime?> { SomeTimestamp.AddDays(1), SomeTimestamp.AddDays(2), SomeTimestamp.AddDays(3), null }
        },
        {
            YdbDbType.Timestamp,
            (DateTime?[])[SomeTimestamp.AddDays(1), SomeTimestamp.AddDays(2), SomeTimestamp.AddDays(3), null]
        },
        {
            YdbDbType.Interval,
            new List<TimeSpan?> { TimeSpan.FromDays(1), TimeSpan.FromDays(2), TimeSpan.FromDays(3), null }
        },
        { YdbDbType.Interval, (TimeSpan?[])[TimeSpan.FromDays(1), TimeSpan.FromDays(2), TimeSpan.FromDays(3), null] }
    };

    public static TheoryData<YdbDbType, IList> ExtraParams = new()
    {
        {
            YdbDbType.Timestamp64, new List<DateTime>
                { SomeTimestamp.AddYears(-100), SomeTimestamp.AddYears(200), SomeTimestamp.AddYears(-300) }
        },
        {
            YdbDbType.Timestamp64,
            (DateTime[])[SomeTimestamp.AddYears(-100), SomeTimestamp.AddYears(200), SomeTimestamp.AddYears(-300)]
        },
        {
            YdbDbType.Timestamp64, new List<DateTime?>
                { SomeTimestamp.AddYears(-100), SomeTimestamp.AddYears(200), SomeTimestamp.AddYears(-300), null }
        },
        {
            YdbDbType.Timestamp64,
            (DateTime?[])[SomeTimestamp.AddYears(-100), SomeTimestamp.AddYears(200), SomeTimestamp.AddYears(-300), null]
        },
        {
            YdbDbType.Datetime64, new List<DateTime>
                { SomeDatetime.AddYears(-100), SomeDatetime.AddYears(200), SomeDatetime.AddYears(-300) }
        },
        {
            YdbDbType.Datetime64,
            (DateTime[])[SomeDatetime.AddYears(-100), SomeDatetime.AddYears(200), SomeDatetime.AddYears(-300)]
        },
        {
            YdbDbType.Datetime64, new List<DateTime?>
                { SomeDatetime.AddYears(-100), SomeDatetime.AddYears(200), SomeDatetime.AddYears(-300), null }
        },
        {
            YdbDbType.Datetime64,
            (DateTime?[])[SomeDatetime.AddYears(-100), SomeDatetime.AddYears(200), SomeDatetime.AddYears(-300), null]
        },
        {
            YdbDbType.Date32, new List<DateTime>
                { SomeDate.AddYears(-100), SomeDate.AddDays(200), SomeDate.AddDays(-300) }
        },
        {
            YdbDbType.Date32,
            (DateTime[])[SomeDate.AddYears(-100), SomeDate.AddDays(200), SomeDate.AddDays(-300)]
        },
        {
            YdbDbType.Date32, new List<DateTime?>
                { SomeDate.AddYears(-100), SomeDate.AddDays(200), SomeDate.AddDays(-300), null }
        },
        {
            YdbDbType.Date32,
            (DateTime?[])[SomeDate.AddYears(-100), SomeDate.AddDays(200), SomeDate.AddDays(-300), null]
        },
        {
            YdbDbType.Datetime, new List<DateTime>
                { SomeDatetime.AddYears(1), SomeTimestamp.AddYears(2), SomeTimestamp.AddYears(3) }
        },
        {
            YdbDbType.Datetime,
            (DateTime[])[SomeDatetime.AddYears(1), SomeTimestamp.AddYears(2), SomeTimestamp.AddYears(3)]
        },
        {
            YdbDbType.Datetime, new List<DateTime?>
                { SomeDatetime.AddYears(1), SomeTimestamp.AddYears(2), SomeTimestamp.AddYears(3), null }
        },
        {
            YdbDbType.Datetime,
            (DateTime?[])[SomeDatetime.AddYears(1), SomeTimestamp.AddYears(2), SomeTimestamp.AddYears(3), null]
        },
        { YdbDbType.Date, new List<DateTime> { SomeDate.AddYears(1), SomeDate.AddYears(2), SomeDate.AddYears(3) } },
        {
            YdbDbType.Date,
            (DateTime[])[SomeDate.AddYears(1), SomeDate.AddYears(2), SomeDate.AddYears(3)]
        },
        {
            YdbDbType.Date,
            new List<DateTime?> { SomeDate.AddYears(1), SomeDate.AddYears(2), SomeDate.AddYears(3), null }
        },
        {
            YdbDbType.Date,
            (DateTime?[])[SomeDate.AddYears(1), SomeDate.AddYears(2), SomeDate.AddYears(3), null]
        },
        {
            YdbDbType.Interval64,
            new List<TimeSpan?> { TimeSpan.FromDays(-1), TimeSpan.FromDays(2), TimeSpan.FromDays(-3), null }
        },
        {
            YdbDbType.Interval64,
            (TimeSpan?[])[TimeSpan.FromDays(-1), TimeSpan.FromDays(2), TimeSpan.FromDays(-3), null]
        },
        {
            YdbDbType.Json,
            new List<string?> { "{\"type\": \"json1\"}", "{\"type\": \"json2\"}", "{\"type\": \"json3\"}", null }
        },
        {
            YdbDbType.Json,
            (string?[])["{\"type\": \"json1\"}", "{\"type\": \"json2\"}", "{\"type\": \"json3\"}", null]
        },
        {
            YdbDbType.JsonDocument,
            new List<string?> { "{\"type\": \"json1\"}", "{\"type\": \"json2\"}", "{\"type\": \"json3\"}", null }
        },
        {
            YdbDbType.JsonDocument,
            (string?[])["{\"type\": \"json1\"}", "{\"type\": \"json2\"}", "{\"type\": \"json3\"}", null]
        },
        {
            YdbDbType.Yson,
            new List<byte[]?> { "{a=1u}"u8.ToArray(), "{a=2u}"u8.ToArray(), null }
        },
        {
            YdbDbType.Yson,
            (byte[]?[])["{a=1u}"u8.ToArray(), "{a=2u}"u8.ToArray(), null]
        },
        { YdbDbType.Int64, new List<object> { 1, 2u, (byte)3 } },
        { YdbDbType.Int64, new object[] { 1, 2u, (byte)3 } } // only not null objects
    };

    [Theory]
    [MemberData(nameof(ListParams))]
    public async Task YdbParameter_SetValue_ArrayOrList_ConvertsToYdbList(YdbDbType ydbDbType, IList list)
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        var testTable = $"auto_cast_ydb_list_{Guid.NewGuid()}";
        var dbTypeStr = ydbDbType == YdbDbType.Decimal ? "Decimal(22, 9)" : ydbDbType.ToString();
        await new YdbCommand(ydbConnection)
                { CommandText = $"CREATE TABLE `{testTable}`(id Uuid, type {dbTypeStr}, PRIMARY KEY(id));" }
            .ExecuteNonQueryAsync();
        await new YdbCommand(ydbConnection)
        {
            CommandText =
                $"INSERT INTO `{testTable}`(id, type) " +
                "SELECT id, type FROM AS_TABLE(ListMap($list, ($x) -> { RETURN <|id: RandomUuid($x), type: $x|> }));",
            Parameters = { new YdbParameter("list", list) }
        }.ExecuteNonQueryAsync();

        var count = await new YdbCommand(ydbConnection)
        {
            CommandText = $"SELECT COUNT(*) FROM `{testTable}` WHERE type IN $list",
            Parameters = { new YdbParameter("list", list) }
        }.ExecuteScalarAsync();

        Assert.Equal(3ul, count);
        Assert.Equal((ulong)list.Count, await new YdbCommand(ydbConnection)
            { CommandText = $"SELECT COUNT(*) FROM `{testTable}`" }.ExecuteScalarAsync());

        await new YdbCommand(ydbConnection) { CommandText = $"DROP TABLE `{testTable}`" }.ExecuteNonQueryAsync();
    }

    [Theory]
    [MemberData(nameof(ListParams))]
    [MemberData(nameof(ExtraParams))]
    public async Task YdbParameter_Value_WithYdbDbTypeList_ProducesListOfSpecifiedType(YdbDbType ydbDbType, IList list)
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        var testTable = $"ydb_list_{Guid.NewGuid()}";
        var dbTypeStr = ydbDbType == YdbDbType.Decimal ? "Decimal(22, 9)" : ydbDbType.ToString();
        await new YdbCommand(ydbConnection)
                { CommandText = $"CREATE TABLE `{testTable}`(id Uuid, type {dbTypeStr}, PRIMARY KEY(id));" }
            .ExecuteNonQueryAsync();
        await new YdbCommand(ydbConnection)
        {
            CommandText =
                $"INSERT INTO `{testTable}`(id, type) " +
                "SELECT id, type FROM AS_TABLE(ListMap($list, ($x) -> { RETURN <|id: RandomUuid($x), type: $x|> }));",
            Parameters = { new YdbParameter("list", YdbDbType.List | ydbDbType, list) }
        }.ExecuteNonQueryAsync();

        if (ydbDbType is not (YdbDbType.Json or YdbDbType.JsonDocument or YdbDbType.Yson))
        {
            Assert.Equal(3ul, await new YdbCommand(ydbConnection)
            {
                CommandText = $"SELECT COUNT(*) FROM `{testTable}` WHERE type IN $list",
                Parameters = { new YdbParameter("list", YdbDbType.List | ydbDbType, list) }
            }.ExecuteScalarAsync());
        }

        Assert.Equal((ulong)list.Count, await new YdbCommand(ydbConnection)
            { CommandText = $"SELECT COUNT(*) FROM `{testTable}`" }.ExecuteScalarAsync());

        await new YdbCommand(ydbConnection) { CommandText = $"DROP TABLE `{testTable}`" }.ExecuteNonQueryAsync();
    }


    [Fact]
    public void YdbParameter_SetValue_ListOrArray_InvalidInputs_Throws()
    {
        Assert.Equal("Writing value of 'System.Object[]' is not supported for parameters having YdbDbType 'List<Bool>'",
            Assert.Throws<InvalidOperationException>(() => new YdbParameter("list",
                YdbDbType.List | YdbDbType.Bool, new object[] { true, false, "string" }).TypedValue).Message);

        Assert.Equal(
            "Writing value of 'System.Object[]' is not supported for parameters having YdbDbType 'List<Decimal>'",
            Assert.Throws<InvalidOperationException>(() => new YdbParameter("list",
                YdbDbType.List | YdbDbType.Decimal, new object[] { 1.0m, false, 2.0m }).TypedValue).Message);

        Assert.Equal("All elements in the list must have the same type. Expected: { \"typeId\": \"INT32\" }, " +
                     "actual: { \"typeId\": \"UINT32\" }", Assert.Throws<ArgumentException>(() =>
            new YdbParameter("list", new List<object> { 1, 2u, (byte)3 }).TypedValue).Message);

        Assert.Equal("All elements in the list must have the same type. Expected: { \"typeId\": \"INT32\" }, " +
                     "actual: { \"typeId\": \"UINT32\" }", Assert.Throws<ArgumentException>(() =>
            new YdbParameter("list", new object[] { 1, 2u, (byte)3 }).TypedValue).Message);

        Assert.Equal("Collection of type 'System.Collections.Generic.List`1[System.Object]' contains null. " +
                     "Specify YdbDbType (e.g. YdbDbType.List | YdbDbType.<T>) " +
                     "or use a strongly-typed collection (e.g., List<T?>).", Assert.Throws<ArgumentException>(() =>
            new YdbParameter("list", new List<object?> { 1, null }).TypedValue).Message);

        Assert.Equal("Collection of type 'System.Object[]' contains null. " +
                     "Specify YdbDbType (e.g. YdbDbType.List | YdbDbType.<T>) " +
                     "or use a strongly-typed collection (e.g., List<T?>).", Assert.Throws<ArgumentException>(() =>
            new YdbParameter("list", new object?[] { 1, null }).TypedValue).Message);
    }

    [Fact]
    public void YdbParameter_SetYdbDbTypeList_Throws() =>
        Assert.Equal("Cannot set YdbDbType to just List. " +
                     "Use Binary-Or with the element type (e.g. Array of dates is YdbDbType.List | YdbDbType.Date). " +
                     "(Parameter 'value')",
            Assert.Throws<ArgumentOutOfRangeException>(() => new YdbParameter("list", YdbDbType.List)).Message);

    [Fact]
    public void YdbParameter_SetYdbDbTypeListWithUnspecified() => Assert.True(
        new YdbParameter("list", YdbDbType.List | YdbDbType.Unspecified).YdbDbType.HasFlag(YdbDbType.List)
    );
}
