using Xunit;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.Tests.Value;

public class BasicUnitTests
{
    [Fact]
    public void PrimitiveTypesMakeGet()
    {
        const bool valueBool = true;
        Assert.Equal(valueBool, YdbValue.MakeBool(valueBool).GetBool());

        const sbyte valueInt8 = -1;
        Assert.Equal(valueInt8, YdbValue.MakeInt8(valueInt8).GetInt8());

        const byte valueUint8 = 200;
        Assert.Equal(valueUint8, YdbValue.MakeUint8(valueUint8).GetUint8());

        const short valueInt16 = -200;
        Assert.Equal(valueInt16, YdbValue.MakeInt16(valueInt16).GetInt16());

        const ushort valueUint16 = 40_000;
        Assert.Equal(valueUint16, YdbValue.MakeUint16(valueUint16).GetUint16());

        const int valueInt32 = -40000;
        Assert.Equal(valueInt32, YdbValue.MakeInt32(valueInt32).GetInt32());

        const uint valueUint32 = 4_000_000_000;
        Assert.Equal(valueUint32, YdbValue.MakeUint32(valueUint32).GetUint32());

        const long valueInt64 = -4_000_000_000;
        Assert.Equal(valueInt64, YdbValue.MakeInt64(valueInt64).GetInt64());

        const ulong valueUint64 = 10_000_000_000ul;
        Assert.Equal(valueUint64, YdbValue.MakeUint64(valueUint64).GetUint64());

        const float valueFloat = -1.7f;
        Assert.Equal(valueFloat, YdbValue.MakeFloat(valueFloat).GetFloat());

        const double valueDouble = 123.45;
        Assert.Equal(valueDouble, YdbValue.MakeDouble(valueDouble).GetDouble());

        var valueDate = new DateTime(2021, 08, 21);
        Assert.Equal(valueDate, YdbValue.MakeDate(valueDate).GetDate());

        var valueDatetime = new DateTime(2021, 08, 21, 23, 30, 47);
        Assert.Equal(valueDatetime, YdbValue.MakeDatetime(valueDatetime).GetDatetime());

        var valueTimestamp = new DateTime(2021, 08, 21, 23, 30, 47, 581);
        Assert.Equal(valueTimestamp, YdbValue.MakeTimestamp(valueTimestamp).GetTimestamp());

        var valueInterval = -new TimeSpan(3, 7, 40, 27, 729);
        Assert.Equal(valueInterval, YdbValue.MakeInterval(valueInterval).GetInterval());

        var valueString = "test str"u8.ToArray();
        Assert.Equal(valueString, YdbValue.MakeString(valueString).GetString());

        const string valueUtf8 = "unicode str";
        Assert.Equal(valueUtf8, YdbValue.MakeUtf8(valueUtf8).GetUtf8());

        var valueYson = "{type=\"yson\"}"u8.ToArray();
        Assert.Equal(valueYson, YdbValue.MakeYson(valueYson).GetYson());

        const string valueJson = "{\"type\": \"json\"}";
        Assert.Equal(valueJson, YdbValue.MakeJson(valueJson).GetJson());

        const string valueJsonDocument = "{\"type\": \"jsondoc\"}";
        Assert.Equal(valueJsonDocument, YdbValue.MakeJsonDocument(valueJsonDocument).GetJsonDocument());
    }

    [Fact]
    public void PrimitiveTypesExplicitCast()
    {
        const bool valueBool = true;
        Assert.Equal(valueBool, (bool)(YdbValue)valueBool);

        const sbyte valueInt8 = -1;
        Assert.Equal(valueInt8, (sbyte)(YdbValue)valueInt8);

        const byte valueUint8 = 200;
        Assert.Equal(valueUint8, (byte)(YdbValue)valueUint8);

        const short valueInt16 = -200;
        Assert.Equal(valueInt16, (short)(YdbValue)valueInt16);

        const ushort valueUint16 = 40_000;
        Assert.Equal(valueUint16, (ushort)(YdbValue)valueUint16);

        const int valueInt32 = -40000;
        Assert.Equal(valueInt32, (int)(YdbValue)valueInt32);

        const uint valueUint32 = 4_000_000_000;
        Assert.Equal(valueUint32, (uint)(YdbValue)valueUint32);

        const long valueInt64 = -4_000_000_000;
        Assert.Equal(valueInt64, (long)(YdbValue)valueInt64);

        const ulong valueUint64 = 10_000_000_000ul;
        Assert.Equal(valueUint64, (ulong)(YdbValue)valueUint64);

        const float valueFloat = -1.7f;
        Assert.Equal(valueFloat, (float)(YdbValue)valueFloat);

        const double valueDouble = 123.45;
        Assert.Equal(valueDouble, (double)(YdbValue)valueDouble);

        var valueInterval = -new TimeSpan(3, 7, 40, 27, 729);
        Assert.Equal(valueInterval, (TimeSpan)(YdbValue)valueInterval);
    }

    [Fact]
    public void OptionalPrimitiveTypesMakeGet()
    {
        const bool valueBool = true;
        Assert.Equal(valueBool, YdbValue.MakeOptionalBool(valueBool).GetOptionalBool());

        const sbyte valueInt8 = -1;
        Assert.Equal(valueInt8, YdbValue.MakeOptionalInt8(valueInt8).GetOptionalInt8());

        const byte valueUint8 = 200;
        Assert.Equal(valueUint8, YdbValue.MakeOptionalUint8(valueUint8).GetOptionalUint8());

        const short valueInt16 = -200;
        Assert.Equal(valueInt16, YdbValue.MakeOptionalInt16(valueInt16).GetOptionalInt16());

        const ushort valueUint16 = 40_000;
        Assert.Equal(valueUint16, YdbValue.MakeOptionalUint16(valueUint16).GetOptionalUint16());

        const int valueInt32 = -40000;
        Assert.Equal(valueInt32, YdbValue.MakeOptionalInt32(valueInt32).GetOptionalInt32());

        const uint valueUint32 = 4_000_000_000;
        Assert.Equal(valueUint32, YdbValue.MakeOptionalUint32(valueUint32).GetOptionalUint32());

        const long valueInt64 = -4_000_000_000;
        Assert.Equal(valueInt64, YdbValue.MakeOptionalInt64(valueInt64).GetOptionalInt64());

        const ulong valueUint64 = 10_000_000_000ul;
        Assert.Equal(valueUint64, YdbValue.MakeOptionalUint64(valueUint64).GetOptionalUint64());

        const float valueFloat = -1.7f;
        Assert.Equal(valueFloat, YdbValue.MakeOptionalFloat(valueFloat).GetOptionalFloat());

        const double valueDouble = 123.45;
        Assert.Equal(valueDouble, YdbValue.MakeOptionalDouble(valueDouble).GetOptionalDouble());

        var valueDate = new DateTime(2021, 08, 21);
        Assert.Equal(valueDate, YdbValue.MakeOptionalDate(valueDate).GetOptionalDate());

        var valueDatetime = new DateTime(2021, 08, 21, 23, 30, 47);
        Assert.Equal(valueDatetime, YdbValue.MakeOptionalDatetime(valueDatetime).GetOptionalDatetime());

        var valueTimestamp = new DateTime(2021, 08, 21, 23, 30, 47, 581);
        Assert.Equal(valueTimestamp, YdbValue.MakeOptionalTimestamp(valueTimestamp).GetOptionalTimestamp());

        var valueInterval = -new TimeSpan(3, 7, 40, 27, 729);
        Assert.Equal(valueInterval, YdbValue.MakeOptionalInterval(valueInterval).GetOptionalInterval());

        var valueString = "test str"u8.ToArray();
        Assert.Equal(valueString, YdbValue.MakeOptionalString(valueString).GetOptionalString());

        const string valueUtf8 = "unicode str";
        Assert.Equal(valueUtf8, YdbValue.MakeOptionalUtf8(valueUtf8).GetOptionalUtf8());

        var valueYson = "{type=\"yson\"}"u8.ToArray();
        Assert.Equal(valueYson, YdbValue.MakeOptionalYson(valueYson).GetOptionalYson());

        const string valueJson = "{\"type\": \"json\"}";
        Assert.Equal(valueJson, YdbValue.MakeOptionalJson(valueJson).GetOptionalJson());

        const string valueJsonDocument = "{\"type\": \"jsondoc\"}";
        Assert.Equal(valueJsonDocument, YdbValue.MakeOptionalJsonDocument(valueJsonDocument).GetOptionalJsonDocument());

        Assert.Null(YdbValue.MakeOptionalBool().GetOptionalBool());
        Assert.Null(YdbValue.MakeOptionalInt8().GetOptionalInt8());
        Assert.Null(YdbValue.MakeOptionalUint8().GetOptionalUint8());
        Assert.Null(YdbValue.MakeOptionalInt16().GetOptionalInt16());
        Assert.Null(YdbValue.MakeOptionalUint16().GetOptionalUint16());
        Assert.Null(YdbValue.MakeOptionalInt32().GetOptionalInt32());
        Assert.Null(YdbValue.MakeOptionalUint32().GetOptionalUint32());
        Assert.Null(YdbValue.MakeOptionalInt64().GetOptionalInt64());
        Assert.Null(YdbValue.MakeOptionalUint64().GetOptionalUint64());
        Assert.Null(YdbValue.MakeOptionalFloat().GetOptionalFloat());
        Assert.Null(YdbValue.MakeOptionalDouble().GetOptionalDouble());
        Assert.Null(YdbValue.MakeOptionalDate().GetOptionalDate());
        Assert.Null(YdbValue.MakeOptionalDatetime().GetOptionalDatetime());
        Assert.Null(YdbValue.MakeOptionalTimestamp().GetOptionalTimestamp());
        Assert.Null(YdbValue.MakeOptionalInterval().GetOptionalInterval());
        Assert.Null(YdbValue.MakeOptionalString().GetOptionalString());
        Assert.Null(YdbValue.MakeOptionalUtf8().GetOptionalUtf8());
        Assert.Null(YdbValue.MakeOptionalYson().GetOptionalYson());
        Assert.Null(YdbValue.MakeOptionalJson().GetOptionalJson());
        Assert.Null(YdbValue.MakeOptionalJsonDocument().GetOptionalJsonDocument());
    }

    [Fact]
    public void OptionalPrimitiveCast()
    {
        bool? valueBool = true;
        Assert.Equal(valueBool, (bool?)(YdbValue)valueBool);

        sbyte? valueInt8 = -1;
        Assert.Equal(valueInt8, (sbyte?)(YdbValue)valueInt8);

        byte? valueUint8 = 200;
        Assert.Equal(valueUint8, (byte?)(YdbValue)valueUint8);

        short? valueInt16 = -200;
        Assert.Equal(valueInt16, (short?)(YdbValue)valueInt16);

        ushort? valueUint16 = 40_000;
        Assert.Equal(valueUint16, (ushort?)(YdbValue)valueUint16);

        int? valueInt32 = -40000;
        Assert.Equal(valueInt32, (int?)(YdbValue)valueInt32);

        uint? valueUint32 = 4_000_000_000;
        Assert.Equal(valueUint32, (uint?)(YdbValue)valueUint32);

        long? valueInt64 = -4_000_000_000;
        Assert.Equal(valueInt64, (long?)(YdbValue)valueInt64);

        ulong? valueUint64 = 10_000_000_000ul;
        Assert.Equal(valueUint64, (ulong?)(YdbValue)valueUint64);

        float? valueFloat = -1.7f;
        Assert.Equal(valueFloat, (float?)(YdbValue)valueFloat);

        double? valueDouble = 123.45;
        Assert.Equal(valueDouble, (double?)(YdbValue)valueDouble);

        TimeSpan? valueInterval = -new TimeSpan(3, 7, 40, 27, 729);
        Assert.Equal(valueInterval, (TimeSpan?)(YdbValue)valueInterval);

        Assert.Null((bool?)(YdbValue)(bool?)null);
        Assert.Null((sbyte?)(YdbValue)(sbyte?)null);
        Assert.Null((byte?)(YdbValue)(byte?)null);
        Assert.Null((short?)(YdbValue)(short?)null);
        Assert.Null((ushort?)(YdbValue)(ushort?)null);
        Assert.Null((int?)(YdbValue)(int?)null);
        Assert.Null((uint?)(YdbValue)(uint?)null);
        Assert.Null((long?)(YdbValue)(long?)null);
        Assert.Null((ulong?)(YdbValue)(ulong?)null);
        Assert.Null((float?)(YdbValue)(float?)null);
        Assert.Null((double?)(YdbValue)(double?)null);
        Assert.Null((TimeSpan?)(YdbValue)(TimeSpan?)null);
    }

    [Fact]
    public void DecimalType()
    {
        (decimal, decimal)[] values =
        [
            (-0.1m, -0.1m),
            (0.0000000000000000000000000001m, 0m),
            (0.0000000000000000000000000000m, 0m),
            (-18446744073.709551616m,
                -18446744073.709551616m), // covers situation when need to add/substract 1 to/from high64
            (123.456m, 123.456m)
        ];
        foreach (var (value, excepted) in values)
        {
            var ydbVal = YdbValue.MakeDecimal(value);
            var result = ydbVal.GetDecimal();
            Assert.Equal(excepted, result);

            Assert.Equal(excepted, YdbValue.MakeDecimal(value).GetDecimal());
            Assert.Equal(excepted, YdbValue.MakeOptionalDecimal(value).GetOptionalDecimal());

            Assert.Equal(excepted, (decimal)(YdbValue)value);
            Assert.Equal(excepted, (decimal?)(YdbValue)(decimal?)value);
        }

        Assert.Null(YdbValue.MakeOptionalDecimal().GetOptionalDecimal());
        Assert.Null((decimal?)(YdbValue)(decimal?)null);

        Assert.Equal("Decimal with precision (30, 0) can't fit into (22, 9)",
            Assert.Throws<InvalidCastException>(() => YdbValue.MakeDecimal(decimal.MaxValue)).Message);
    }

    [Fact]
    public void DecimalTypeWithPrecision()
    {
        Assert.Equal(12345m, YdbValue.MakeDecimal(12345m).GetDecimal());
        Assert.Equal(12345m, YdbValue.MakeDecimalWithPrecision(12345m, precision: 5, scale: 0).GetDecimal());
        Assert.Equal(12345m, YdbValue.MakeDecimalWithPrecision(12345m, precision: 7, scale: 2).GetDecimal());
        Assert.Equal(123.46m, YdbValue.MakeDecimalWithPrecision(123.456m, precision: 5, scale: 2).GetDecimal());
        Assert.Equal(-18446744073.709551616m,
            YdbValue.MakeDecimal(-18446744073.709551616m).GetDecimal());
        Assert.Equal(-18446744073.709551616m,
            YdbValue.MakeDecimalWithPrecision(-18446744073.709551616m, precision: 21, scale: 9).GetDecimal());
        Assert.Equal(-18446744074m,
            YdbValue.MakeDecimalWithPrecision(-18446744073.709551616m, precision: 12, scale: 0).GetDecimal());
        Assert.Equal(-184467440730709551616m,
            YdbValue.MakeDecimalWithPrecision(-184467440730709551616m, precision: 21, scale: 0).GetDecimal());
    }

    [Fact]
    public void ListType()
    {
        var value = YdbValue.MakeTuple(new[]
        {
            YdbValue.MakeEmptyList(YdbTypeId.Int32),
            YdbValue.MakeList(new[] { YdbValue.MakeUtf8("one"), YdbValue.MakeUtf8("two") })
        });

        var elements = value.GetTuple();
        Assert.Equal(2, elements.Count);

        Assert.Empty(elements[0].GetList());
        Assert.Equal(["one", "two"], elements[1].GetList().Select(v => (string)v!));
    }

    [Fact]
    public void TupleType()
    {
        var value = YdbValue.MakeTuple(new[] { YdbValue.MakeTuple([]) });

        var elements = value.GetTuple();
        Assert.Single(elements);

        Assert.Empty(elements[0].GetTuple());
    }

    [Fact]
    public void StructType()
    {
        var value = YdbValue.MakeTuple(new[]
        {
            YdbValue.MakeStruct(new Dictionary<string, YdbValue>()),
            YdbValue.MakeStruct(new Dictionary<string, YdbValue>
            {
                { "Member1", YdbValue.MakeInt64(10) },
                { "Member2", YdbValue.MakeUtf8("ten") }
            })
        });

        var elements = value.GetTuple();
        Assert.Equal(2, elements.Count);

        Assert.Empty(elements[0].GetStruct());

        var s = elements[1].GetStruct();
        Assert.Equal(2, s.Count);
        Assert.Equal(10, (long)s["Member1"]);
        Assert.Equal("ten", (string)s["Member2"]!);
    }
}
