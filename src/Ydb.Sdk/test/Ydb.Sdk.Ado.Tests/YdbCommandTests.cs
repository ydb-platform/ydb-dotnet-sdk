using System.Collections;
using System.Data;
using Xunit;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.Tests;

public class YdbCommandTests : TestBase
{
    [Theory]
    [ClassData(typeof(TestDataGenerator))]
    public async Task ExecuteScalarAsync_WhenSetYdbParameter_ReturnThisValue<T>(Data<T> data)
    {
        await using var connection = await CreateOpenConnectionAsync();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = "SELECT @var as var;";

        var dbParameter = new YdbParameter
        {
            ParameterName = "var",
            DbType = data.DbType,
            Value = data.Expected,
            IsNullable = data.IsNullable
        };

        dbCommand.Parameters.Add(dbParameter);

        Assert.Equal(data.Expected == null ? DBNull.Value : data.Expected, await dbCommand.ExecuteScalarAsync());
        var ydbDataReader = await dbCommand.ExecuteReaderAsync();
        Assert.Equal(1, ydbDataReader.FieldCount);
        Assert.Equal("var", ydbDataReader.GetName(0));
        if (!data.IsNullable)
        {
            Assert.Equal(typeof(T), ydbDataReader.GetFieldType(0));
        }

        while (await ydbDataReader.NextResultAsync())
        {
        }
    }

    [Theory]
    [ClassData(typeof(TestDataGenerator))]
    public async Task ExecuteScalarAsync_WhenSetYdbParameterThenPrepare_ReturnThisValue<T>(Data<T> data)
    {
        await using var connection = await CreateOpenConnectionAsync();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = "SELECT @var;";

        var dbParameter = new YdbParameter
        {
            ParameterName = "@var",
            DbType = data.DbType,
            Value = data.Expected,
            IsNullable = data.IsNullable
        };
        dbCommand.Parameters.Add(dbParameter);

        Assert.Equal(data.Expected == null ? DBNull.Value : data.Expected, await dbCommand.ExecuteScalarAsync());
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
    [ClassData(typeof(TestDataGenerator))]
    public async Task ExecuteScalarAsync_WhenDbTypeIsObject_ReturnThisValue<T>(Data<T> data)
    {
        if (data.IsNullable)
        {
            return;
        }

        await using var connection = await CreateOpenConnectionAsync();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = "SELECT @var;";

        var dbParameter = new YdbParameter
        {
            ParameterName = "@var",
            Value = data.Expected,
            IsNullable = data.IsNullable
        };
        dbCommand.Parameters.Add(dbParameter);

        Assert.Equal(data.Expected, await dbCommand.ExecuteScalarAsync());
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

    public class Data<T>(DbType dbType, T expected, bool isNullable = false)
    {
        public bool IsNullable { get; } = isNullable || expected == null;
        public DbType DbType { get; } = dbType;
        public T Expected { get; } = expected;
    }

    private class TestDataGenerator : IEnumerable<object[]>
    {
        private readonly List<object[]> _data =
        [
            new object[] { new Data<bool>(DbType.Boolean, true) },
            new object[] { new Data<bool>(DbType.Boolean, false) },
            new object[] { new Data<bool?>(DbType.Boolean, true, true) },
            new object[] { new Data<bool?>(DbType.Boolean, false, true) },
            new object[] { new Data<bool?>(DbType.Boolean, null) },
            new object[] { new Data<sbyte>(DbType.SByte, -1) },
            new object[] { new Data<sbyte?>(DbType.SByte, -2, true) },
            new object[] { new Data<sbyte?>(DbType.SByte, null) },
            new object[] { new Data<byte>(DbType.Byte, 200) },
            new object[] { new Data<byte?>(DbType.Byte, 228, true) },
            new object[] { new Data<byte?>(DbType.Byte, null) },
            new object[] { new Data<short>(DbType.Int16, 14000) },
            new object[] { new Data<short?>(DbType.Int16, 14000, true) },
            new object[] { new Data<short?>(DbType.Int16, null) },
            new object[] { new Data<ushort>(DbType.UInt16, 40_000) },
            new object[] { new Data<ushort?>(DbType.UInt16, 40_000, true) },
            new object[] { new Data<ushort?>(DbType.UInt16, null) },
            new object[] { new Data<int>(DbType.Int32, -40_000) },
            new object[] { new Data<int?>(DbType.Int32, -40_000, true) },
            new object[] { new Data<int?>(DbType.Int32, null) },
            new object[] { new Data<uint>(DbType.UInt32, 4_000_000_000) },
            new object[] { new Data<uint?>(DbType.UInt32, 4_000_000_000, true) },
            new object[] { new Data<uint?>(DbType.UInt32, null) },
            new object[] { new Data<long>(DbType.Int64, -4_000_000_000) },
            new object[] { new Data<long?>(DbType.Int64, -4_000_000_000, true) },
            new object[] { new Data<long?>(DbType.Int64, null) },
            new object[] { new Data<ulong>(DbType.UInt64, 10_000_000_000ul) },
            new object[] { new Data<ulong?>(DbType.UInt64, 10_000_000_000ul, true) },
            new object[] { new Data<ulong?>(DbType.UInt64, null) },
            new object[] { new Data<float>(DbType.Single, -1.7f) },
            new object[] { new Data<float?>(DbType.Single, -1.7f, true) },
            new object[] { new Data<float?>(DbType.Single, null) },
            new object[] { new Data<double>(DbType.Double, 123.45) },
            new object[] { new Data<double?>(DbType.Double, 123.45, true) },
            new object[] { new Data<double?>(DbType.Double, null) },
            new object[] { new Data<Guid>(DbType.Guid, new Guid("6E73B41C-4EDE-4D08-9CFB-B7462D9E498B")) },
            new object[] { new Data<Guid?>(DbType.Guid, new Guid("6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"), true) },
            new object[] { new Data<Guid?>(DbType.Guid, null) },
            new object[] { new Data<DateTime>(DbType.Date, new DateTime(2021, 08, 21)) },
            new object[] { new Data<DateTime?>(DbType.Date, new DateTime(2021, 08, 21), true) },
            new object[] { new Data<DateTime?>(DbType.Date, null) },
            new object[] { new Data<DateTime>(DbType.DateTime, new DateTime(2021, 08, 21, 23, 30, 47)) },
            new object[] { new Data<DateTime?>(DbType.DateTime, new DateTime(2021, 08, 21, 23, 30, 47), true) },
            new object[] { new Data<DateTime?>(DbType.DateTime, null) },
            new object[] { new Data<DateTime>(DbType.DateTime2, DateTime.Parse("2029-08-03T06:59:44.8578730Z")) },
            new object[] { new Data<DateTime>(DbType.DateTime2, DateTime.Parse("2029-08-09T17:15:29.6935850Z")) },
            new object[]
            {
                new Data<DateTime?>(DbType.DateTime2, new DateTime(2021, 08, 21, 23, 30, 47, 581, DateTimeKind.Local),
                    true)
            },
            new object[] { new Data<DateTime?>(DbType.DateTime2, null) },
            new object[] { new Data<byte[]>(DbType.Binary, "test str"u8.ToArray()) },
            new object[] { new Data<byte[]?>(DbType.Binary, "test str"u8.ToArray(), true) },
            new object[] { new Data<byte[]?>(DbType.Binary, null) },
            new object[] { new Data<string>(DbType.String, "unicode str") },
            new object[] { new Data<string?>(DbType.String, "unicode str", true) },
            new object[] { new Data<string?>(DbType.String, null) },
            new object[] { new Data<decimal>(DbType.Decimal, -18446744073.709551616m) },
            new object[] { new Data<decimal?>(DbType.Decimal, -18446744073.709551616m, true) },
            new object[] { new Data<decimal?>(DbType.Decimal, null) }
        ];

        public IEnumerator<object[]> GetEnumerator() => _data.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
