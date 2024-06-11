using System.Collections;
using System.Data;
using System.Text;
using Xunit;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Tests.Ado;

[Trait("Category", "Unit")]
public class YdbParameterTests
{
    [Fact]
    public void YdbValue_WhenValueIsNullAndDbTypeIsObject_ThrowException()
    {
        Assert.Equal("Error converting null to YdbValue", Assert.Throws<YdbAdoException>(() =>
            new YdbParameter().YdbValue).Message);
        Assert.Equal("Error converting System.Object to YdbValue", Assert.Throws<YdbAdoException>(() =>
            new YdbParameter("$param", new object()).YdbValue).Message);
    }

    [Theory]
    [ClassData(typeof(TestDataGenerator))]
    public void YdbValue_WhenSetDbValue_ReturnYdbValue<T>(Data<T> data)
    {
        Assert.Equal(data.Expected, data.FetchFun(new YdbParameter("$parameter", data.DbType, data.Expected)
            { IsNullable = data.IsNullable }.YdbValue));

        if (!data.IsNullable && data.DbType != DbType.DateTime2 && data.DbType != DbType.Date && data.Expected != null)
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
            { IsNullable = true }.YdbValue.GetOptionalTimestamp());
    }

    [Fact]
    public void YdbValue_WhenYdbValueIsSet_ReturnThis()
    {
        Assert.Equal("{\"type\": \"jsondoc\"}",
            new YdbParameter("$parameter", YdbValue.MakeJsonDocument("{\"type\": \"jsondoc\"}")).YdbValue
                .GetJsonDocument());
    }

    [Theory]
    [InlineData(DbType.VarNumeric, "VarNumeric")]
    [InlineData(DbType.Xml, "Xml")]
    [InlineData(DbType.Guid, "Guid")]
    public void YdbValue_WhenNoSupportedDbType_ThrowException(DbType dbType, string name)
    {
        Assert.Equal("Ydb don't supported this DbType: " + name,
            Assert.Throws<YdbAdoException>(() => new YdbParameter("$parameter", dbType)
                { IsNullable = true }.YdbValue).Message);
    }

    [Fact]
    public void Parameter_WhenSetAndNoSet_ReturnValueOrException()
    {
        Assert.Equal("$parameter", new YdbParameter { ParameterName = "$parameter" }.ParameterName);
        Assert.Equal("ParameterName must not be null!",
            Assert.Throws<YdbAdoException>(() => new YdbParameter { ParameterName = null }).Message);
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

    private class TestDataGenerator : IEnumerable<object[]>
    {
        private readonly List<object[]> _data = new()
        {
            new object[] { new Data<bool>(DbType.Boolean, true, value => value.GetBool()) },
            new object[] { new Data<bool>(DbType.Boolean, false, value => value.GetBool()) },
            new object[] { new Data<bool?>(DbType.Boolean, true, value => value.GetOptionalBool(), true) },
            new object[] { new Data<bool?>(DbType.Boolean, false, value => value.GetOptionalBool(), true) },
            new object[] { new Data<bool?>(DbType.Boolean, null, value => value.GetOptionalBool()) },
            new object[] { new Data<sbyte>(DbType.SByte, -1, value => value.GetInt8()) },
            new object[] { new Data<sbyte?>(DbType.SByte, -2, value => value.GetOptionalInt8(), true) },
            new object[] { new Data<sbyte?>(DbType.SByte, null, value => value.GetOptionalInt8()) },
            new object[] { new Data<byte>(DbType.Byte, 200, value => value.GetUint8()) },
            new object[] { new Data<byte?>(DbType.Byte, 228, value => value.GetOptionalUint8(), true) },
            new object[] { new Data<byte?>(DbType.Byte, null, value => value.GetOptionalUint8()) },
            new object[] { new Data<short>(DbType.Int16, 14000, value => value.GetInt16()) },
            new object[] { new Data<short?>(DbType.Int16, 14000, value => value.GetOptionalInt16(), true) },
            new object[] { new Data<short?>(DbType.Int16, null, value => value.GetOptionalInt16()) },
            new object[] { new Data<ushort>(DbType.UInt16, 40_000, value => value.GetUint16()) },
            new object[] { new Data<ushort?>(DbType.UInt16, 40_000, value => value.GetOptionalUint16(), true) },
            new object[] { new Data<ushort?>(DbType.UInt16, null, value => value.GetOptionalUint16()) },
            new object[] { new Data<int>(DbType.Int32, -40_000, value => value.GetInt32()) },
            new object[] { new Data<int?>(DbType.Int32, -40_000, value => value.GetOptionalInt32(), true) },
            new object[] { new Data<int?>(DbType.Int32, null, value => value.GetOptionalInt32()) },
            new object[] { new Data<uint>(DbType.UInt32, 4_000_000_000, value => value.GetUint32()) },
            new object[] { new Data<uint?>(DbType.UInt32, 4_000_000_000, value => value.GetOptionalUint32(), true) },
            new object[] { new Data<uint?>(DbType.UInt32, null, value => value.GetOptionalUint32()) },
            new object[] { new Data<long>(DbType.Int64, -4_000_000_000, value => value.GetInt64()) },
            new object[] { new Data<long?>(DbType.Int64, -4_000_000_000, value => value.GetOptionalInt64(), true) },
            new object[] { new Data<long?>(DbType.Int64, null, value => value.GetOptionalInt64()) },
            new object[] { new Data<ulong>(DbType.UInt64, 10_000_000_000ul, value => value.GetUint64()) },
            new object[]
                { new Data<ulong?>(DbType.UInt64, 10_000_000_000ul, value => value.GetOptionalUint64(), true) },
            new object[] { new Data<ulong?>(DbType.UInt64, null, value => value.GetOptionalUint64()) },
            new object[] { new Data<float>(DbType.Single, -1.7f, value => value.GetFloat()) },
            new object[] { new Data<float?>(DbType.Single, -1.7f, value => value.GetOptionalFloat(), true) },
            new object[] { new Data<float?>(DbType.Single, null, value => value.GetOptionalFloat()) },
            new object[] { new Data<double>(DbType.Double, 123.45, value => value.GetDouble()) },
            new object[] { new Data<double?>(DbType.Double, 123.45, value => value.GetOptionalDouble(), true) },
            new object[] { new Data<double?>(DbType.Double, null, value => value.GetOptionalDouble()) },
            new object[] { new Data<DateTime>(DbType.Date, new DateTime(2021, 08, 21), value => value.GetDate()) },
            new object[]
            {
                new Data<DateTime?>(DbType.Date, new DateTime(2021, 08, 21), value => value.GetOptionalDate(), true)
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
                    value => value.GetOptionalDatetime(), true)
            },
            new object[] { new Data<DateTime?>(DbType.DateTime, null, value => value.GetOptionalDatetime()) },
            new object[]
            {
                new Data<DateTime>(DbType.Time, new DateTime(2021, 08, 21, 23, 30, 47),
                    value => value.GetDatetime())
            },
            new object[]
            {
                new Data<DateTime?>(DbType.Time, new DateTime(2021, 08, 21, 23, 30, 47),
                    value => value.GetOptionalDatetime(), true)
            },
            new object[] { new Data<DateTime?>(DbType.Time, null, value => value.GetOptionalDatetime()) },
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
                    value => value.GetOptionalTimestamp(), true)
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
                    value => value.GetOptionalString(), true)
            },
            new object[] { new Data<byte[]?>(DbType.Binary, null, value => value.GetOptionalString()) },
            new object[] { new Data<string>(DbType.String, "unicode str", value => value.GetUtf8()) },
            new object[] { new Data<string?>(DbType.String, "unicode str", value => value.GetOptionalUtf8(), true) },
            new object[] { new Data<string?>(DbType.String, null, value => value.GetOptionalUtf8()) },
            new object[] { new Data<decimal>(DbType.Decimal, -18446744073.709551616m, value => value.GetDecimal()) },
            new object[]
            {
                new Data<decimal?>(DbType.Decimal, -18446744073.709551616m, value => value.GetOptionalDecimal(), true)
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
