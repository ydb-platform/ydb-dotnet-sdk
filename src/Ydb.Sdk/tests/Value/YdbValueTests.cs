using System.Collections;
using System.Text;
using Xunit;
using Ydb.Sdk.Tests.Fixture;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Tests.Value;

[Trait("Category", "Integration")]
public class YdbValueTests : IClassFixture<TableClientFixture>
{
    private readonly TableClientFixture _tableClientFixture;

    public YdbValueTests(TableClientFixture tableClientFixture)
    {
        _tableClientFixture = tableClientFixture;
    }

    private async Task<YdbValue> SelectPassed(YdbValue value)
    {
        var response = await Utils.ExecuteDataQuery(
            _tableClientFixture.TableClient,
            "SELECT $value;",
            new Dictionary<string, YdbValue> { { "$value", value } });
        Assert.True(response.Result.ResultSets.Count == 1);
        Assert.True(response.Result.ResultSets[0].Rows.Count == 1);
        var row = response.Result.ResultSets[0].Rows[0];
        return row[0];
    }

    public class Data<T>
    {
        public Data(T expected, Func<T, YdbValue> makeFun, Func<YdbValue, T> fetchFun)
        {
            Expected = expected;
            YdbValue = makeFun(expected);
            FetchFun = fetchFun;
        }

        public T Expected { get; }

        public YdbValue YdbValue { get; }

        public Func<YdbValue, T> FetchFun { get; }
    }

    private class TestDataGenerator : IEnumerable<object[]>
    {
        private readonly List<object[]> _data = new()
        {
            new object[] { new Data<bool>(true, YdbValue.MakeBool, value => value.GetBool()) },
            new object[] { new Data<bool>(false, YdbValue.MakeBool, value => value.GetBool()) },
            new object[] { new Data<bool?>(true, YdbValue.MakeOptionalBool, value => value.GetOptionalBool()) },
            new object[] { new Data<bool?>(false, YdbValue.MakeOptionalBool, value => value.GetOptionalBool()) },
            new object[] { new Data<bool?>(null, YdbValue.MakeOptionalBool, value => value.GetOptionalBool()) },
            new object[] { new Data<sbyte>(-1, YdbValue.MakeInt8, value => value.GetInt8()) },
            new object[] { new Data<sbyte?>(-2, YdbValue.MakeOptionalInt8, value => value.GetOptionalInt8()) },
            new object[] { new Data<sbyte?>(null, YdbValue.MakeOptionalInt8, value => value.GetOptionalInt8()) },
            new object[] { new Data<byte>(200, YdbValue.MakeUint8, value => value.GetUint8()) },
            new object[] { new Data<byte?>(228, YdbValue.MakeOptionalUint8, value => value.GetOptionalUint8()) },
            new object[] { new Data<byte?>(null, YdbValue.MakeOptionalUint8, value => value.GetOptionalUint8()) },
            new object[] { new Data<short>(14000, YdbValue.MakeInt16, value => value.GetInt16()) },
            new object[] { new Data<short?>(14000, YdbValue.MakeOptionalInt16, value => value.GetOptionalInt16()) },
            new object[] { new Data<short?>(null, YdbValue.MakeOptionalInt16, value => value.GetOptionalInt16()) },
            new object[] { new Data<ushort>(40_000, YdbValue.MakeUint16, value => value.GetUint16()) },
            new object[] { new Data<ushort?>(40_000, YdbValue.MakeOptionalUint16, value => value.GetOptionalUint16()) },
            new object[] { new Data<ushort?>(null, YdbValue.MakeOptionalUint16, value => value.GetOptionalUint16()) },
            new object[] { new Data<int>(-40_000, YdbValue.MakeInt32, value => value.GetInt32()) },
            new object[] { new Data<int?>(-40_000, YdbValue.MakeOptionalInt32, value => value.GetOptionalInt32()) },
            new object[] { new Data<int?>(null, YdbValue.MakeOptionalInt32, value => value.GetOptionalInt32()) },
            new object[] { new Data<uint>(4_000_000_000, YdbValue.MakeUint32, value => value.GetUint32()) },
            new object[]
                { new Data<uint?>(4_000_000_000, YdbValue.MakeOptionalUint32, value => value.GetOptionalUint32()) },
            new object[] { new Data<uint?>(null, YdbValue.MakeOptionalUint32, value => value.GetOptionalUint32()) },
            new object[] { new Data<long>(-4_000_000_000, YdbValue.MakeInt64, value => value.GetInt64()) },
            new object[]
                { new Data<long?>(-4_000_000_000, YdbValue.MakeOptionalInt64, value => value.GetOptionalInt64()) },
            new object[] { new Data<long?>(null, YdbValue.MakeOptionalInt64, value => value.GetOptionalInt64()) },
            new object[] { new Data<ulong>(10_000_000_000ul, YdbValue.MakeUint64, value => value.GetUint64()) },
            new object[]
                { new Data<ulong?>(10_000_000_000ul, YdbValue.MakeOptionalUint64, value => value.GetOptionalUint64()) },
            new object[] { new Data<ulong?>(null, YdbValue.MakeOptionalUint64, value => value.GetOptionalUint64()) },
            new object[] { new Data<float>(-1.7f, YdbValue.MakeFloat, value => value.GetFloat()) },
            new object[] { new Data<float?>(-1.7f, YdbValue.MakeOptionalFloat, value => value.GetOptionalFloat()) },
            new object[] { new Data<float?>(null, YdbValue.MakeOptionalFloat, value => value.GetOptionalFloat()) },
            new object[] { new Data<double>(123.45, YdbValue.MakeDouble, value => value.GetDouble()) },
            new object[] { new Data<double?>(123.45, YdbValue.MakeOptionalDouble, value => value.GetOptionalDouble()) },
            new object[] { new Data<double?>(null, YdbValue.MakeOptionalDouble, value => value.GetOptionalDouble()) },
            new object[]
                { new Data<DateTime>(new DateTime(2021, 08, 21), YdbValue.MakeDate, value => value.GetDate()) },
            new object[]
            {
                new Data<DateTime?>(new DateTime(2021, 08, 21), YdbValue.MakeOptionalDate,
                    value => value.GetOptionalDate())
            },
            new object[] { new Data<DateTime?>(null, YdbValue.MakeOptionalDate, value => value.GetOptionalDate()) },
            new object[]
            {
                new Data<DateTime>(new DateTime(2021, 08, 21, 23, 30, 47), YdbValue.MakeDatetime,
                    value => value.GetDatetime())
            },
            new object[]
            {
                new Data<DateTime?>(new DateTime(2021, 08, 21, 23, 30, 47), YdbValue.MakeOptionalDatetime,
                    value => value.GetOptionalDatetime())
            },
            new object[]
                { new Data<DateTime?>(null, YdbValue.MakeOptionalDatetime, value => value.GetOptionalDatetime()) },
            new object[]
            {
                new Data<DateTime>(DateTime.Parse("2029-08-03T06:59:44.8578730Z"), YdbValue.MakeTimestamp,
                    value => value.GetTimestamp())
            },
            new object[]
            {
                new Data<DateTime>(DateTime.Parse("2029-08-09T17:15:29.6935850Z"), YdbValue.MakeTimestamp,
                    value => value.GetTimestamp())
            },
            new object[]
            {
                new Data<DateTime?>(new DateTime(2021, 08, 21, 23, 30, 47, 581, DateTimeKind.Local),
                    YdbValue.MakeOptionalTimestamp, value => value.GetOptionalTimestamp())
            },
            new object[]
                { new Data<DateTime?>(null, YdbValue.MakeOptionalTimestamp, value => value.GetOptionalTimestamp()) },
            new object[]
            {
                new Data<TimeSpan>(-new TimeSpan(3, 7, 40, 27, 729), YdbValue.MakeInterval,
                    value => value.GetInterval())
            },
            new object[]
            {
                new Data<TimeSpan?>(-new TimeSpan(3, 7, 40, 27, 729), YdbValue.MakeOptionalInterval,
                    value => value.GetOptionalInterval())
            },
            new object[]
                { new Data<TimeSpan?>(null, YdbValue.MakeOptionalInterval, value => value.GetOptionalInterval()) },
            new object[]
            {
                new Data<byte[]>(Encoding.ASCII.GetBytes("test str"), YdbValue.MakeString, value => value.GetString())
            },
            new object[]
            {
                new Data<byte[]?>(Encoding.ASCII.GetBytes("test str"), YdbValue.MakeOptionalString,
                    value => value.GetOptionalString())
            },
            new object[] { new Data<byte[]?>(null, YdbValue.MakeOptionalString, value => value.GetOptionalString()) },
            new object[] { new Data<string>("unicode str", YdbValue.MakeUtf8, value => value.GetUtf8()) },
            new object[]
                { new Data<string?>("unicode str", YdbValue.MakeOptionalUtf8, value => value.GetOptionalUtf8()) },
            new object[] { new Data<string?>(null, YdbValue.MakeOptionalUtf8, value => value.GetOptionalUtf8()) },
            new object[]
            {
                new Data<byte[]>(Encoding.ASCII.GetBytes("{type=\"yson\"}"), YdbValue.MakeYson,
                    value => value.GetYson())
            },
            new object[]
            {
                new Data<byte[]?>(Encoding.ASCII.GetBytes("{type=\"yson\"}"), YdbValue.MakeOptionalYson,
                    value => value.GetOptionalYson())
            },
            new object[] { new Data<byte[]?>(null, YdbValue.MakeOptionalYson, value => value.GetOptionalYson()) },
            new object[] { new Data<string>("{\"type\": \"json\"}", YdbValue.MakeJson, value => value.GetJson()) },
            new object[]
            {
                new Data<string?>("{\"type\": \"json\"}", YdbValue.MakeOptionalJson, value => value.GetOptionalJson())
            },
            new object[] { new Data<string?>(null, YdbValue.MakeOptionalJson, value => value.GetOptionalJson()) },
            new object[]
            {
                new Data<string>("{\"type\":\"jsondoc\"}", YdbValue.MakeJsonDocument, value => value.GetJsonDocument())
            },
            new object[]
            {
                new Data<string?>("{\"type\":\"jsondoc\"}", YdbValue.MakeOptionalJsonDocument,
                    value => value.GetOptionalJsonDocument())
            },
            new object[]
                { new Data<string?>(null, YdbValue.MakeOptionalJsonDocument, value => value.GetOptionalJsonDocument()) }
        };

        public IEnumerator<object[]> GetEnumerator() => _data.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }


    [Theory]
    [ClassData(typeof(TestDataGenerator))]
    public async Task YdbValue_SelectPrimitiveType_ReturnThisPrimitiveValue<T>(Data<T> data)
    {
        var response = await SelectPassed(data.YdbValue);

        Assert.Equal(data.Expected, data.FetchFun(response));
    }


    [Fact]
    public async Task DecimalTypeSelectPassed()
    {
        var testData = new[] { 12345m, 12.345m, 12.34m, 12m, -18446744073.709551616m };

        foreach (var expected in testData)
        {
            var response = await SelectPassed(YdbValue.MakeDecimal(expected));
            var actual = response.GetDecimal();
            Assert.Equal(expected, actual);
        }
    }

    [Fact]
    public async Task DecimalTypeSelectPassedWithPrecision()
    {
        var testData = new[]
        {
            YdbValue.MakeDecimalWithPrecision(12345m),
            YdbValue.MakeDecimalWithPrecision(12345m, precision: 5, scale: 0),
            YdbValue.MakeDecimalWithPrecision(12345m, precision: 7, scale: 2),
            YdbValue.MakeDecimalWithPrecision(-18446744073.709551616m),
            YdbValue.MakeDecimalWithPrecision(-18446744073.709551616m, precision: 21, scale: 9),
            YdbValue.MakeDecimalWithPrecision(-184467440730709551616m, precision: 21, scale: 0),
            YdbValue.MakeDecimalWithPrecision(-18446744073.709551616m, precision: 12, scale: 0)
        };
        foreach (var expected in testData)
        {
            var response = await SelectPassed(expected);
            var actual = response.GetDecimal();
            Assert.Equal(expected.GetDecimal(), actual);
        }
    }

    private async Task PrepareDecimalTable()
    {
        const string query = @"
CREATE TABLE decimal_test
(
    key Uint64,
    value Decimal(22,9),
    PRIMARY KEY (key)
);
";
        await Utils.ExecuteSchemeQuery(_tableClientFixture.TableClient, query);
    }

    private async Task UpsertAndCheckDecimal(ulong key, decimal value)
    {
        const string query = @"
UPSERT INTO decimal_test (key, value) 
VALUES ($key, $value);

SELECT value FROM decimal_test WHERE key = $key;
";

        var parameters = new Dictionary<string, YdbValue>
        {
            { "$key", (YdbValue)key },
            { "$value", (YdbValue)value }
        };

        var response = await Utils.ExecuteDataQuery(_tableClientFixture.TableClient, query, parameters);

        var resultSet = response.Result.ResultSets[0];

        var ydbValue = resultSet.Rows[0][0];
        var result = ydbValue.GetOptionalDecimal();
        Assert.Equal(value, result);
    }

    [Fact]
    public async Task DecimalTypeRw()
    {
        await PrepareDecimalTable();

        var testData = new (ulong, decimal)[]
        {
            (1, 1m),
            (2, 123.456m),
            (3, -1m),
            (4, -0.1m),
            (5, 0.000000000m),
            (6, 0.000000001m),
            (7, -18446744073.709551616m)
        };

        foreach (var (key, value) in testData)
        {
            await UpsertAndCheckDecimal(key, value);
        }
    }

    [Fact]
    public async Task ListType()
    {
        var value = new[] { YdbValue.MakeUtf8("one"), YdbValue.MakeUtf8("two") };
        var emptyList = YdbValue.MakeEmptyList(YdbTypeId.Int32);
        var list = YdbValue.MakeList(value);

        var emptyListResult = await SelectPassed(emptyList);
        var listResult = await SelectPassed(list);

        Assert.Empty(emptyListResult.GetList());
        Assert.Equal(new[] { "one", "two" }, listResult.GetList().Select(v => (string)v!));
    }


    [Fact]
    public async Task TupleType()
    {
        var value = YdbValue.MakeTuple(new[]
        {
            YdbValue.MakeTuple(new YdbValue[] { }),
            YdbValue.MakeInt32(123)
        });

        var result = await SelectPassed(value);

        Assert.Equal(2, result.GetTuple().Count);
        Assert.Empty(result.GetTuple()[0].GetTuple());
        Assert.Equal(123, result.GetTuple()[1].GetInt32());
    }


    [Fact]
    public async Task StructType()
    {
        var value = YdbValue.MakeStruct(new Dictionary<string, YdbValue>
        {
            { "foo1", YdbValue.MakeStruct(new Dictionary<string, YdbValue>()) },
            {
                "foo2", YdbValue.MakeStruct(new Dictionary<string, YdbValue>
                {
                    { "bar1", YdbValue.MakeInt64(10) },
                    { "bar2", YdbValue.MakeUtf8("ten") }
                })
            }
        });

        var result = await SelectPassed(value);

        Assert.Equal(2, result.GetStruct().Count);

        var foo1 = result.GetStruct()["foo1"].GetStruct();
        var foo2 = result.GetStruct()["foo2"].GetStruct();

        Assert.Empty(foo1);
        Assert.Equal(2, foo2.Count);

        Assert.Equal(10, foo2["bar1"].GetInt64());
        Assert.Equal("ten", foo2["bar2"].GetUtf8());
    }
}
