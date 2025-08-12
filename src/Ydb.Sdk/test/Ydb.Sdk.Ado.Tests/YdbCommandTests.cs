using System.Collections;
using System.Data;
using System.Text;
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
            (YdbValue.MakeYson("{type=\"yson\"}"u8.ToArray()), "{type=\"yson\"}"u8.ToArray()),
            (YdbValue.MakeOptionalJson(simpleJson), simpleJson),
            (YdbValue.MakeOptionalJsonDocument(simpleJson), simpleJson),
            (YdbValue.MakeOptionalInterval(TimeSpan.FromSeconds(5)), TimeSpan.FromSeconds(5)),
            (YdbValue.MakeOptionalYson("{type=\"yson\"}"u8.ToArray()), "{type=\"yson\"}"u8.ToArray())
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
    

    public class Data<T>
    {
        public Data(DbType dbType, T expected, Func<YdbValue, T> fetchFun, bool isNullable = false)
        {
            DbType = dbType;
            Expected = expected;
            IsNullable = isNullable || expected == null;
            FetchFun = fetchFun;
        }

        public bool IsNullable { get; }
        public DbType DbType { get; }
        public T Expected { get; }
        public Func<YdbValue, T> FetchFun { get; }
    }

    public class TestDataGenerator : IEnumerable<object[]>
    {
        private readonly List<object[]> _data =
        [
            new object[] { new Data<bool>(DbType.Boolean, true, value => value.GetBool()) },
            new object[] { new Data<bool>(DbType.Boolean, false, value => value.GetBool()) },
            new object[] { new Data<bool?>(DbType.Boolean, true, value => value.GetBool(), true) },
            new object[] { new Data<bool?>(DbType.Boolean, false, value => value.GetBool(), true) },
            new object[] { new Data<bool?>(DbType.Boolean, null, value => value.GetOptionalBool()) },
            new object[] { new Data<sbyte>(DbType.SByte, -1, value => value.GetInt8()) },
            new object[] { new Data<sbyte?>(DbType.SByte, -2, value => value.GetInt8(), true) },
            new object[] { new Data<sbyte?>(DbType.SByte, null, value => value.GetOptionalInt8()) },
            new object[] { new Data<byte>(DbType.Byte, 200, value => value.GetUint8()) },
            new object[] { new Data<byte?>(DbType.Byte, 228, value => value.GetUint8(), true) },
            new object[] { new Data<byte?>(DbType.Byte, null, value => value.GetOptionalUint8()) },
            new object[] { new Data<short>(DbType.Int16, 14000, value => value.GetInt16()) },
            new object[] { new Data<short?>(DbType.Int16, 14000, value => value.GetInt16(), true) },
            new object[] { new Data<short?>(DbType.Int16, null, value => value.GetOptionalInt16()) },
            new object[] { new Data<ushort>(DbType.UInt16, 40_000, value => value.GetUint16()) },
            new object[] { new Data<ushort?>(DbType.UInt16, 40_000, value => value.GetUint16(), true) },
            new object[] { new Data<ushort?>(DbType.UInt16, null, value => value.GetOptionalUint16()) },
            new object[] { new Data<int>(DbType.Int32, -40_000, value => value.GetInt32()) },
            new object[] { new Data<int?>(DbType.Int32, -40_000, value => value.GetInt32(), true) },
            new object[] { new Data<int?>(DbType.Int32, null, value => value.GetOptionalInt32()) },
            new object[] { new Data<uint>(DbType.UInt32, 4_000_000_000, value => value.GetUint32()) },
            new object[] { new Data<uint?>(DbType.UInt32, 4_000_000_000, value => value.GetUint32(), true) },
            new object[] { new Data<uint?>(DbType.UInt32, null, value => value.GetOptionalUint32()) },
            new object[] { new Data<long>(DbType.Int64, -4_000_000_000, value => value.GetInt64()) },
            new object[] { new Data<long?>(DbType.Int64, -4_000_000_000, value => value.GetInt64(), true) },
            new object[] { new Data<long?>(DbType.Int64, null, value => value.GetOptionalInt64()) },
            new object[] { new Data<ulong>(DbType.UInt64, 10_000_000_000ul, value => value.GetUint64()) },
            new object[]
                { new Data<ulong?>(DbType.UInt64, 10_000_000_000ul, value => value.GetUint64(), true) },

            new object[] { new Data<ulong?>(DbType.UInt64, null, value => value.GetOptionalUint64()) },
            new object[] { new Data<float>(DbType.Single, -1.7f, value => value.GetFloat()) },
            new object[] { new Data<float?>(DbType.Single, -1.7f, value => value.GetFloat(), true) },
            new object[] { new Data<float?>(DbType.Single, null, value => value.GetOptionalFloat()) },
            new object[] { new Data<double>(DbType.Double, 123.45, value => value.GetDouble()) },
            new object[] { new Data<double?>(DbType.Double, 123.45, value => value.GetDouble(), true) },
            new object[] { new Data<double?>(DbType.Double, null, value => value.GetOptionalDouble()) },
            new object[]
            {
                new Data<Guid>(DbType.Guid, new Guid("6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"),
                    value => value.GetUuid())
            },

            new object[]
            {
                new Data<Guid?>(DbType.Guid, new Guid("6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"),
                    value => value.GetUuid(), true)
            },

            new object[] { new Data<Guid?>(DbType.Guid, null, value => value.GetOptionalUuid()) },
            new object[] { new Data<DateTime>(DbType.Date, new DateTime(2021, 08, 21), value => value.GetDate()) },
            new object[]
            {
                new Data<DateTime?>(DbType.Date, new DateTime(2021, 08, 21), value => value.GetDate(), true)
            },

            new object[] { new Data<DateTime?>(DbType.Date, null, value => value.GetOptionalDate()) },
            new object[]
            {
                new Data<DateTime>(DbType.DateTime, new DateTime(2021, 08, 21, 23, 30, 47),
                    value => value.GetDatetime())
            },

            new object[]
            {
                new Data<DateTime?>(DbType.DateTime, new DateTime(2021, 08, 21, 23, 30, 47),
                    value => value.GetDatetime(), true)
            },

            new object[] { new Data<DateTime?>(DbType.DateTime, null, value => value.GetOptionalDatetime()) },
            new object[]
            {
                new Data<DateTime>(DbType.DateTime2, DateTime.Parse("2029-08-03T06:59:44.8578730Z"),
                    value => value.GetTimestamp())
            },

            new object[]
            {
                new Data<DateTime>(DbType.DateTime2, DateTime.Parse("2029-08-09T17:15:29.6935850Z"),
                    value => value.GetTimestamp())
            },

            new object[]
            {
                new Data<DateTime?>(DbType.DateTime2, new DateTime(2021, 08, 21, 23, 30, 47, 581, DateTimeKind.Local),
                    value => value.GetTimestamp(), true)
            },

            new object[] { new Data<DateTime?>(DbType.DateTime2, null, value => value.GetOptionalTimestamp()) },
            new object[]
            {
                new Data<byte[]>(DbType.Binary, Encoding.ASCII.GetBytes("test str"),
                    value => value.GetString())
            },

            new object[]
            {
                new Data<byte[]?>(DbType.Binary, Encoding.ASCII.GetBytes("test str"),
                    value => value.GetString(), true)
            },

            new object[] { new Data<byte[]?>(DbType.Binary, null, value => value.GetOptionalString()) },
            new object[] { new Data<string>(DbType.String, "unicode str", value => value.GetUtf8()) },
            new object[] { new Data<string?>(DbType.String, "unicode str", value => value.GetUtf8(), true) },
            new object[] { new Data<string?>(DbType.String, null, value => value.GetOptionalUtf8()) },
            new object[] { new Data<decimal>(DbType.Decimal, -18446744073.709551616m, value => value.GetDecimal()) },
            new object[]
            {
                new Data<decimal?>(DbType.Decimal, -18446744073.709551616m, value => value.GetDecimal(), true)
            },

            new object[] { new Data<decimal?>(DbType.Decimal, null, value => value.GetOptionalDecimal()) }
        ];

        public IEnumerator<object[]> GetEnumerator() => _data.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
