using System.Data;
using Xunit;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.Tests;

public class YdbCommandTests : TestBase
{
    [Theory]
    [MemberData(nameof(DbTypeTestCases))]
    [MemberData(nameof(DbTypeTestNullCases))]
    public async Task ExecuteScalarAsync_WhenSetYdbParameter_ReturnThisValue(DbType dbType, object? value,
        bool isNullable)
    {
        await using var connection = await CreateOpenConnectionAsync();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = "SELECT @var as var;";

        var dbParameter = new YdbParameter
            { ParameterName = "var", DbType = dbType, Value = value, IsNullable = isNullable };

        dbCommand.Parameters.Add(dbParameter);

        Assert.Equal(value ?? DBNull.Value, await dbCommand.ExecuteScalarAsync());
        var ydbDataReader = await dbCommand.ExecuteReaderAsync();
        Assert.Equal(1, ydbDataReader.FieldCount);
        Assert.Equal("var", ydbDataReader.GetName(0));
        if (value != null)
        {
            Assert.Equal(value.GetType(), ydbDataReader.GetFieldType(0));
        }

        while (await ydbDataReader.NextResultAsync())
        {
        }
    }

    [Theory]
    [MemberData(nameof(DbTypeTestCases))]
    [MemberData(nameof(DbTypeTestNullCases))]
    public async Task ExecuteScalarAsync_WhenSetYdbParameterThenPrepare_ReturnThisValue(DbType dbType, object? value,
        bool isNullable)
    {
        await using var connection = await CreateOpenConnectionAsync();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = "SELECT @var;";

        var dbParameter = new YdbParameter
            { ParameterName = "@var", DbType = dbType, Value = value, IsNullable = isNullable };
        dbCommand.Parameters.Add(dbParameter);

        Assert.Equal(value ?? DBNull.Value, await dbCommand.ExecuteScalarAsync());
    }

    [Fact]
    public async Task ExecuteReaderAsync_WhenSelectNull_ThrowFieldIsNull()
    {
        await using var connection = await CreateOpenConnectionAsync();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = "SELECT NULL";
        var reader = await dbCommand.ExecuteReaderAsync();
        await reader.ReadAsync();
        Assert.Equal("Field is null.", Assert.Throws<InvalidCastException>(() => reader.GetFloat(0)).Message);
    }

    [Fact]
    public async Task ExecuteReaderAsync_WhenOptionalIsNull_ThrowFieldIsNull()
    {
        await using var connection = await CreateOpenConnectionAsync();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = "SELECT CAST(NULL AS Optional<Float>) AS Field";
        var reader = await dbCommand.ExecuteReaderAsync();
        await reader.ReadAsync();

        Assert.Equal("Field is null.", Assert.Throws<InvalidCastException>(() => reader.GetFloat(0)).Message);
    }

    [Theory]
    [MemberData(nameof(DbTypeTestCases))]
    public async Task ExecuteScalarAsync_WhenDbTypeIsObject_ReturnThisValue(DbType _, object value, bool isNullable)
    {
        await using var connection = await CreateOpenConnectionAsync();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = "SELECT @var;";

        var dbParameter = new YdbParameter
        {
            ParameterName = "@var",
            Value = value,
            IsNullable = isNullable
        };
        dbCommand.Parameters.Add(dbParameter);

        Assert.Equal(value, await dbCommand.ExecuteScalarAsync());
    }

    [Fact]
    public async Task ExecuteScalarAsync_WhenNoDbTypeParameter_ReturnThisValue()
    {
        const string simpleJson = @"{""a"":""b""}";

        var args = new List<(YdbValue YdbValue, object Expected)>
        {
            (YdbValue.MakeJson(simpleJson), simpleJson),
            (YdbValue.MakeJsonDocument(simpleJson), simpleJson),
            (YdbValue.MakeInterval(TimeSpan.FromSeconds(5)), TimeSpan.FromSeconds(5)),
            (YdbValue.MakeOptionalJson(simpleJson), simpleJson),
            (YdbValue.MakeOptionalJsonDocument(simpleJson), simpleJson),
            (YdbValue.MakeOptionalInterval(TimeSpan.FromSeconds(5)), TimeSpan.FromSeconds(5))
        };

        await using var connection = await CreateOpenConnectionAsync();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = "SELECT @var;";

        foreach (var arg in args)
        {
            dbCommand.Parameters.Clear();
            dbCommand.Parameters.Add(new YdbParameter
            {
                ParameterName = "@var",
                Value = arg.YdbValue
            });
            Assert.Equal(arg.Expected, await dbCommand.ExecuteScalarAsync());
        }
    }

    [Fact]
    public async Task CloseAsync_WhenDoubleInvoke_Idempotent()
    {
        await using var connection = await CreateOpenConnectionAsync();
        var ydbCommand = connection.CreateCommand();
        ydbCommand.CommandText = "SELECT 1;";
        var ydbDataReader = await ydbCommand.ExecuteReaderAsync();

        Assert.False(await ydbDataReader.NextResultAsync());
        await ydbDataReader.CloseAsync();
        await ydbDataReader.CloseAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => ydbDataReader.NextResultAsync());
    }

    [Fact]
    public void CommandTimeout_WhenCommandTimeoutLessZero_ThrowException()
    {
        using var connection = CreateOpenConnection();
        var dbCommand = connection.CreateCommand();
        Assert.Equal("CommandTimeout can't be less than zero. (Parameter 'value')\nActual value was -1.",
            Assert.Throws<ArgumentOutOfRangeException>(() => dbCommand.CommandTimeout = -1).Message);
    }

    [Fact]
    public void ExecuteDbDataReader_WhenPreviousIsNotClosed_ThrowException()
    {
        using var connection = CreateOpenConnection();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = "SELECT 1; SELECT 1;";
        var ydbDataReader = dbCommand.ExecuteReader();

        Assert.Equal("A command is already in progress: SELECT 1; SELECT 1;",
            Assert.Throws<YdbOperationInProgressException>(() => dbCommand.ExecuteReader()).Message);
        Assert.True(ydbDataReader.NextResult());
        Assert.False(ydbDataReader.NextResult());

        ydbDataReader.Close();
        Assert.True(ydbDataReader.IsClosed);
    }

    [Fact]
    public async Task ExecuteScalar_WhenSelectNull_ReturnDbNull()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        Assert.Equal(DBNull.Value,
            await new YdbCommand(ydbConnection) { CommandText = "SELECT NULL" }.ExecuteScalarAsync());
    }

    [Fact]
    public async Task GetValue_WhenSelectNull_ReturnDbNull()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        var reader = await new YdbCommand(ydbConnection) { CommandText = "SELECT NULL" }.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.True(reader.IsDBNull(0));
        Assert.Equal(DBNull.Value, reader.GetValue(0));
    }

    [Fact]
    public async Task ExecuteScalar_WhenSelectNoRows_ReturnNull()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        Assert.Null(await new YdbCommand(ydbConnection) { CommandText = "SELECT * FROM (select 1) AS T WHERE FALSE" }
            .ExecuteScalarAsync());
    }

    [Fact]
    public async Task ExecuteReaderAsync_WhenParamsHaveDifferentTypes_ThrowArgumentException()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        var ex = await Assert.ThrowsAsync<ArgumentException>(() => new YdbCommand(ydbConnection)
        {
            CommandText = "SELECT * FROM T WHERE Ids in (@id1, @id2);",
            Parameters =
            {
                new YdbParameter("id1", DbType.String, "text"),
                new YdbParameter("id2", DbType.Int32, 1)
            }
        }.ExecuteReaderAsync());
        Assert.Equal("All elements in the list must have the same type. " +
                     "Expected: { \"typeId\": \"UTF8\" }, actual: { \"typeId\": \"INT32\" }", ex.Message);
    }

    [Fact]
    public async Task ExecuteReaderAsync_WhenParamsHaveNullOrNotNullTypes_ThrowArgumentException()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        var ex = await Assert.ThrowsAsync<ArgumentException>(() => new YdbCommand(ydbConnection)
        {
            CommandText = "SELECT * FROM T WHERE Ids in (@id1, @id2);",
            Parameters =
            {
                new YdbParameter("id1", DbType.Int32),
                new YdbParameter("id2", DbType.Int32, 1)
            }
        }.ExecuteReaderAsync());
        Assert.Equal("All elements in the list must have the same type. " +
                     "Expected: { \"optionalType\": { \"item\": { \"typeId\": \"INT32\" } } }, " +
                     "actual: { \"typeId\": \"INT32\" }", ex.Message);
    }

    [Fact]
    public async Task ExecuteReaderAsync_WhenEmptyList_ReturnEmptyResultSet()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        var tempTable = $"temp_table_{Guid.NewGuid()}";
        await new YdbCommand(ydbConnection) { CommandText = $"CREATE TABLE `{tempTable}` (a Int32, PRIMARY KEY (a));" }
            .ExecuteNonQueryAsync();
        await new YdbCommand(ydbConnection) { CommandText = $"INSERT INTO `{tempTable}` (a) VALUES (1);" }
            .ExecuteNonQueryAsync();
        var reader = await new YdbCommand(ydbConnection) { CommandText = $"SELECT * FROM `{tempTable}` WHERE a IN ()" }
            .ExecuteReaderAsync();
        Assert.False(await reader.ReadAsync());
        await new YdbCommand(ydbConnection) { CommandText = $"DROP TABLE `{tempTable}`" }.ExecuteNonQueryAsync();
    }

    public class Data<T>(DbType dbType, T expected, bool isNullable = false)
    {
        public bool IsNullable { get; } = isNullable || expected == null;
        public DbType DbType { get; } = dbType;
        public T Expected { get; } = expected;
    }


    public static readonly TheoryData<DbType, object, bool> DbTypeTestCases = new()
    {
        { DbType.Boolean, true, false },
        { DbType.Boolean, false, false },
        { DbType.Boolean, true, true },
        { DbType.Boolean, false, true },
        { DbType.SByte, (sbyte)-1, false },
        { DbType.SByte, (sbyte)-2, true },
        { DbType.Byte, (byte)200, false },
        { DbType.Byte, (byte)228, true },
        { DbType.Int16, (short)14000, false },
        { DbType.Int16, (short)14000, true },
        { DbType.UInt16, (ushort)40_000, false },
        { DbType.UInt16, (ushort)40_000, true },
        { DbType.Int32, -40_000, false },
        { DbType.Int32, -40_000, true },
        { DbType.UInt32, 4_000_000_000, true },
        { DbType.UInt32, 4_000_000_000, true },
        { DbType.Int64, -4_000_000_000, false },
        { DbType.Int64, -4_000_000_000, true },
        { DbType.UInt64, 10_000_000_000ul, false },
        { DbType.UInt64, 10_000_000_000ul, true },
        { DbType.Single, -1.7f, false },
        { DbType.Single, -1.7f, true },
        { DbType.Double, 123.45, false },
        { DbType.Double, 123.45, true },
        { DbType.Guid, new Guid("6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"), false },
        { DbType.Guid, new Guid("6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"), true },
        { DbType.Date, new DateTime(2021, 08, 21), false },
        { DbType.Date, new DateTime(2021, 08, 21), true },
        { DbType.DateTime, new DateTime(2021, 08, 21, 23, 30, 47), false },
        { DbType.DateTime, new DateTime(2021, 08, 21, 23, 30, 47), true },
        { DbType.DateTime2, DateTime.Parse("2029-08-03T06:59:44.8578730Z"), false },
        { DbType.DateTime2, DateTime.Parse("2029-08-09T17:15:29.6935850Z"), false },
        { DbType.DateTime2, new DateTime(2021, 08, 21, 23, 30, 47, 581, DateTimeKind.Local), true },
        { DbType.Binary, "test str"u8.ToArray(), false },
        { DbType.Binary, "test str"u8.ToArray(), true },
        { DbType.String, "unicode str", false },
        { DbType.String, "unicode str", true },
        { DbType.Decimal, -18446744073.709551616m, false },
        { DbType.Decimal, -18446744073.709551616m, true },
    };

    public static readonly TheoryData<DbType, object?, bool> DbTypeTestNullCases = new()
    {
        { DbType.Boolean, null, false },
        { DbType.Boolean, null, true },
        { DbType.SByte, null, false },
        { DbType.SByte, null, true },
        { DbType.Byte, null, false },
        { DbType.Byte, null, true },
        { DbType.Int16, null, false },
        { DbType.Int16, null, true },
        { DbType.UInt16, null, false },
        { DbType.UInt16, null, true },
        { DbType.Int32, null, false },
        { DbType.Int32, null, true },
        { DbType.UInt32, null, false },
        { DbType.UInt32, null, true },
        { DbType.Int64, null, false },
        { DbType.Int64, null, true },
        { DbType.UInt64, null, false },
        { DbType.UInt64, null, true },
        { DbType.Single, null, false },
        { DbType.Single, null, true },
        { DbType.Double, null, false },
        { DbType.Double, null, true },
        { DbType.Guid, null, false },
        { DbType.Guid, null, true },
        { DbType.Date, null, false },
        { DbType.Date, null, true },
        { DbType.DateTime, null, false },
        { DbType.DateTime, null, true },
        { DbType.DateTime2, null, false },
        { DbType.DateTime2, null, true },
        { DbType.Binary, null, false },
        { DbType.Binary, null, true },
        { DbType.String, null, false },
        { DbType.String, null, true },
        { DbType.Decimal, null, false },
        { DbType.Decimal, null, true }
    };
}
