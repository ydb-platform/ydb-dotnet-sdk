using System.Collections;
using System.Data;
using System.Text;
using Xunit;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Tests.Ado;

public class YdbParameterTests
{
    [Fact]
    public void YdbValue_WhenValueIsNullAndDbTypeIsObject_ThrowException()
    {
        Assert.Equal("Writing value of 'null' is not supported for parameters having DbType 'Object'",
            Assert.Throws<InvalidOperationException>(() => new YdbParameter().YdbValue).Message);
        Assert.Equal("Writing value of 'System.Object' is not supported for parameters having DbType 'Object'",
            Assert.Throws<InvalidOperationException>(() => new YdbParameter("$param", new object()).YdbValue).Message);
    }

    [Fact]
    public void ParameterName_WhenSetWithoutAnyFormat_ReturnCorrectName()
    {
        Assert.Equal("$name", new YdbParameter { ParameterName = "name" }.ParameterName);
        Assert.Equal("$name", new YdbParameter { ParameterName = "@name" }.ParameterName);
        Assert.Equal("$name", new YdbParameter { ParameterName = "$name" }.ParameterName);
    }

    [Theory]
    [ClassData(typeof(TestDataGenerator))]
    public void YdbValue_WhenSetDbValue_ReturnYdbValue<T>(Data<T> data)
    {
        Assert.Equal(data.Expected, data.FetchFun(new YdbParameter("$parameter", data.DbType, data.Expected)
            { IsNullable = data.IsNullable }.YdbValue));

        if (!data.IsNullable && data.DbType != DbType.DateTime && data.DbType != DbType.Date && data.Expected != null)
        {
            Assert.Equal(data.Expected, data.FetchFun(new YdbParameter("$parameter", data.Expected).YdbValue));
        }
    }

    [Fact]
    public void YdbValue_WhenDateTimeOffset_ReturnTimestamp()
    {
        var dateTimeOffset = DateTimeOffset.Parse("2029-08-03T06:59:44.8578730Z");

        Assert.Equal(dateTimeOffset.UtcDateTime,
            new YdbParameter("$parameter", dateTimeOffset).YdbValue.GetTimestamp());
        Assert.Equal(dateTimeOffset.UtcDateTime,
            new YdbParameter("$parameter", DbType.DateTimeOffset, dateTimeOffset).YdbValue.GetTimestamp());
        Assert.Null(new YdbParameter("$parameter", DbType.DateTimeOffset) { IsNullable = true }
            .YdbValue.GetOptionalTimestamp());
        Assert.Equal(dateTimeOffset.UtcDateTime, new YdbParameter("$parameter", DbType.DateTimeOffset, dateTimeOffset)
            { IsNullable = true }.YdbValue.GetTimestamp());
    }

    [Fact]
    public void YdbValue_WhenYdbValueIsSet_ReturnThis()
    {
        Assert.Equal("{\"type\": \"jsondoc\"}",
            new YdbParameter("$parameter", YdbValue.MakeJsonDocument("{\"type\": \"jsondoc\"}")).YdbValue
                .GetJsonDocument());
    }

    [Fact]
    public void YdbValue_WhenUnCastTypes_ThrowInvalidCastException()
    {
        Assert.Equal("Writing value of 'System.Int32' is not supported for parameters having DbType 'Boolean'",
            Assert.Throws<InvalidOperationException>(() => new YdbParameter("$var", DbType.Boolean, 1).YdbValue).Message);
        Assert.Equal("Writing value of 'System.Int32' is not supported for parameters having DbType 'SByte'",
            Assert.Throws<InvalidOperationException>(() => new YdbParameter("$var", DbType.SByte, 1).YdbValue).Message);
        Assert.Equal("Writing value of 'System.String' is not supported for parameters having DbType 'Boolean'",
            Assert.Throws<InvalidOperationException>(() => new YdbParameter("$parameter", DbType.Boolean)
                { Value = "true" }.YdbValue).Message);
        Assert.Equal("Writing value of 'System.Double' is not supported for parameters having DbType 'Single'",
            Assert.Throws<InvalidOperationException>(() => new YdbParameter("$var", DbType.Single, 1.1).YdbValue).Message);
    }

    [Theory]
    [InlineData(DbType.VarNumeric, "VarNumeric")]
    [InlineData(DbType.Xml, "Xml")]
    [InlineData(DbType.Time, "Time")]
    public void YdbValue_WhenNoSupportedDbType_ThrowException(DbType dbType, string name)
    {
        Assert.Equal("Ydb don't supported this DbType: " + name,
            Assert.Throws<YdbException>(() => new YdbParameter("$parameter", dbType)
                { IsNullable = true }.YdbValue).Message);
    }

    [Fact]
    public void Parameter_WhenSetAndNoSet_ReturnValueOrException()
    {
        Assert.Equal("$parameter", new YdbParameter { ParameterName = "$parameter" }.ParameterName);
        Assert.Equal(string.Empty, new YdbParameter { ParameterName = null }.ParameterName);
    }

    [Fact]
    public void YdbValue_WhenSetDbType_ReturnConvertValue()
    {
        Assert.Equal(1U, new YdbParameter("$parameter", DbType.UInt64) { Value = 1U }.YdbValue.GetUint64());
        Assert.Equal(1U, new YdbParameter("$parameter", DbType.UInt64) { Value = (ushort)1U }.YdbValue.GetUint64());
        Assert.Equal(1U, new YdbParameter("$parameter", DbType.UInt64) { Value = (byte)1U }.YdbValue.GetUint64());

        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int64) { Value = 1 }.YdbValue.GetInt64());
        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int64) { Value = (uint)1 }.YdbValue.GetInt64());
        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int64) { Value = (ushort)1 }.YdbValue.GetInt64());
        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int64) { Value = (byte)1 }.YdbValue.GetInt64());
        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int64) { Value = (short)1 }.YdbValue.GetInt64());
        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int64) { Value = (sbyte)1 }.YdbValue.GetInt64());

        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int32) { Value = (ushort)1 }.YdbValue.GetInt32());
        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int32) { Value = (byte)1 }.YdbValue.GetInt32());
        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int32) { Value = (short)1 }.YdbValue.GetInt32());
        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int32) { Value = (sbyte)1 }.YdbValue.GetInt32());

        Assert.Equal(1U, new YdbParameter("$parameter", DbType.UInt32) { Value = (ushort)1 }.YdbValue.GetUint32());
        Assert.Equal(1U, new YdbParameter("$parameter", DbType.UInt32) { Value = (byte)1 }.YdbValue.GetUint32());

        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int16) { Value = (byte)1 }.YdbValue.GetInt16());
        Assert.Equal(1, new YdbParameter("$parameter", DbType.Int16) { Value = (sbyte)1 }.YdbValue.GetInt16());

        Assert.Equal(1U, new YdbParameter("$parameter", DbType.UInt16) { Value = (byte)1 }.YdbValue.GetUint16());

        Assert.Equal(1.1f, new YdbParameter("$parameter", DbType.Double) { Value = 1.1f }.YdbValue.GetDouble());
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
        private readonly List<object[]> _data = new()
        {
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
                new Data<byte[]>(DbType.Binary, Encoding.ASCII.GetBytes("test str").ToArray(),
                    value => value.GetString())
            },
            new object[]
            {
                new Data<byte[]?>(DbType.Binary, Encoding.ASCII.GetBytes("test str").ToArray(),
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
        };

        public IEnumerator<object[]> GetEnumerator()
        {
            return _data.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
