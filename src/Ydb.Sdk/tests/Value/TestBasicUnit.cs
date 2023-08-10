#if NETCOREAPP3_1
using System;
using System.Collections.Generic;
using System.Linq;
#endif
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace Ydb.Sdk.Value.Tests
{
    [Trait("Category", "Unit")]
    public class TestBasicUnit
    {
        private readonly ITestOutputHelper _output;

        public TestBasicUnit(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void PrimitiveTypesMakeGet()
        {
            var valueBool = true;
            var valueInt8 = (sbyte)-1;
            var valueUint8 = (byte)200;
            var valueInt16 = (short)-200;
            var valueUint16 = (ushort)40_000;
            var valueInt32 = -40000;
            var valueUint32 = 4_000_000_000;
            var valueInt64 = -4_000_000_000;
            var valueUint64 = 10_000_000_000ul;
            var valueFloat = -1.7f;
            var valueDouble = 123.45;
            var valueDate = new DateTime(2021, 08, 21);
            var valueDatetime = new DateTime(2021, 08, 21, 23, 30, 47);
            var valueTimestamp = new DateTime(2021, 08, 21, 23, 30, 47, 581);
            var valueInterval = -new TimeSpan(3, 7, 40, 27, 729);
            var valueString = Encoding.ASCII.GetBytes("test str");
            var valueUtf8 = "unicode str";
            var valueYson = Encoding.ASCII.GetBytes("{type=\"yson\"}");
            var valueJson = "{\"type\": \"json\"}";
            var valueJsonDocument = "{\"type\": \"jsondoc\"}";

            Assert.Equal(valueBool, YdbValue.MakeBool(valueBool).GetBool());
            Assert.Equal(valueInt8, YdbValue.MakeInt8(valueInt8).GetInt8());
            Assert.Equal(valueUint8, YdbValue.MakeUint8(valueUint8).GetUint8());
            Assert.Equal(valueInt16, YdbValue.MakeInt16(valueInt16).GetInt16());
            Assert.Equal(valueUint16, YdbValue.MakeUint16(valueUint16).GetUint16());
            Assert.Equal(valueInt32, YdbValue.MakeInt32(valueInt32).GetInt32());
            Assert.Equal(valueUint32, YdbValue.MakeUint32(valueUint32).GetUint32());
            Assert.Equal(valueInt64, YdbValue.MakeInt64(valueInt64).GetInt64());
            Assert.Equal(valueUint64, YdbValue.MakeUint64(valueUint64).GetUint64());
            Assert.Equal(valueFloat, YdbValue.MakeFloat(valueFloat).GetFloat());
            Assert.Equal(valueDouble, YdbValue.MakeDouble(valueDouble).GetDouble());
            Assert.Equal(valueDate, YdbValue.MakeDate(valueDate).GetDate());
            Assert.Equal(valueDatetime, YdbValue.MakeDatetime(valueDatetime).GetDatetime());
            Assert.Equal(valueTimestamp, YdbValue.MakeTimestamp(valueTimestamp).GetTimestamp());
            Assert.Equal(valueInterval, YdbValue.MakeInterval(valueInterval).GetInterval());
            Assert.Equal(valueString, YdbValue.MakeString(valueString).GetString());
            Assert.Equal(valueUtf8, YdbValue.MakeUtf8(valueUtf8).GetUtf8());
            Assert.Equal(valueYson, YdbValue.MakeYson(valueYson).GetYson());
            Assert.Equal(valueJson, YdbValue.MakeJson(valueJson).GetJson());
            Assert.Equal(valueJsonDocument, YdbValue.MakeJsonDocument(valueJsonDocument).GetJsonDocument());

        }

        [Fact]
        public void PrimitiveTypesExplicitCast() {

            var valueBool = true;
            var valueInt8 = (sbyte)-1;
            var valueUint8 = (byte)200;
            var valueInt16 = (short)-200;
            var valueUint16 = (ushort)40_000;
            var valueInt32 = -40000;
            var valueUint32 = 4_000_000_000;
            var valueInt64 = -4_000_000_000;
            var valueUint64 = 10_000_000_000ul;
            var valueFloat = -1.7f;
            var valueDouble = 123.45;

            Assert.Equal(valueBool, (bool)(YdbValue)valueBool);
            Assert.Equal(valueInt8, (sbyte)(YdbValue)valueInt8);
            Assert.Equal(valueUint8, (byte)(YdbValue)valueUint8);
            Assert.Equal(valueInt16, (short)(YdbValue)valueInt16);
            Assert.Equal(valueUint16, (ushort)(YdbValue)valueUint16);
            Assert.Equal(valueInt32, (int)(YdbValue)valueInt32);
            Assert.Equal(valueUint32, (uint)(YdbValue)valueUint32);
            Assert.Equal(valueInt64, (long)(YdbValue)valueInt64);
            Assert.Equal(valueUint64, (ulong)(YdbValue)valueUint64);
            Assert.Equal(valueFloat, (float)(YdbValue)valueFloat);
            Assert.Equal(valueDouble, (double)(YdbValue)valueDouble);
        }

        [Fact]
        public void OptimalPrimitiveTypesMakeGet()
        {
            var valueBool = true;
            var valueInt8 = (sbyte)-1;
            var valueUint8 = (byte)200;
            var valueInt16 = (short)-200;
            var valueUint16 = (ushort)40_000;
            var valueInt32 = -40000;
            var valueUint32 = 4_000_000_000;
            var valueInt64 = -4_000_000_000;
            var valueUint64 = 10_000_000_000ul;
            var valueFloat = -1.7f;
            var valueDouble = 123.45;
            var valueDate = new DateTime(2021, 08, 21);
            var valueDatetime = new DateTime(2021, 08, 21, 23, 30, 47);
            var valueTimestamp = new DateTime(2021, 08, 21, 23, 30, 47, 581);
            var valueInterval = -new TimeSpan(3, 7, 40, 27, 729);

            Assert.Equal(valueBool, YdbValue.MakeOptionalBool(valueBool).GetOptionalBool());
            Assert.Equal(valueInt8, YdbValue.MakeOptionalInt8(valueInt8).GetOptionalInt8());
            Assert.Equal(valueUint8, YdbValue.MakeOptionalUint8(valueUint8).GetOptionalUint8());
            Assert.Equal(valueInt16, YdbValue.MakeOptionalInt16(valueInt16).GetOptionalInt16());
            Assert.Equal(valueUint16, YdbValue.MakeOptionalUint16(valueUint16).GetOptionalUint16());
            Assert.Equal(valueInt32, YdbValue.MakeOptionalInt32(valueInt32).GetOptionalInt32());
            Assert.Equal(valueUint32, YdbValue.MakeOptionalUint32(valueUint32).GetOptionalUint32());
            Assert.Equal(valueInt64, YdbValue.MakeOptionalInt64(valueInt64).GetOptionalInt64());
            Assert.Equal(valueUint64, YdbValue.MakeOptionalUint64(valueUint64).GetOptionalUint64());
            Assert.Equal(valueFloat, YdbValue.MakeOptionalFloat(valueFloat).GetOptionalFloat());
            Assert.Equal(valueDouble, YdbValue.MakeOptionalDouble(valueDouble).GetOptionalDouble());
            Assert.Equal(valueDate, YdbValue.MakeOptionalDate(valueDate).GetOptionalDate());
            Assert.Equal(valueDatetime, YdbValue.MakeOptionalDatetime(valueDatetime).GetOptionalDatetime());
            Assert.Equal(valueTimestamp, YdbValue.MakeOptionalTimestamp(valueTimestamp).GetOptionalTimestamp());
            Assert.Equal(valueInterval, YdbValue.MakeOptionalInterval(valueInterval).GetOptionalInterval());

            // TODO make optional string types
        }

        [Fact]
        public void OptimalPrimitiveCast()
        {
            var valueBool = (bool?)true;
            var valueInt8 = (sbyte?)-1;
            var valueUint8 = (byte?)200;
            var valueInt16 = (short?)-200;
            var valueUint16 = (ushort?)40_000;
            var valueInt32 = (int?)-40000;
            var valueUint32 = (uint?)4_000_000_000;
            var valueInt64 = (long?)-4_000_000_000;
            var valueUint64 = (ulong?)10_000_000_000ul;
            var valueFloat = (float?)-1.7f;
            var valueDouble = (double?)123.45;

            Assert.Equal(valueBool, (bool?)(YdbValue)valueBool);
            Assert.Equal(valueInt8, (sbyte?)(YdbValue)valueInt8);
            Assert.Equal(valueUint8, (byte?)(YdbValue)valueUint8);
            Assert.Equal(valueInt16, (short?)(YdbValue)valueInt16);
            Assert.Equal(valueUint16, (ushort?)(YdbValue)valueUint16);
            Assert.Equal(valueInt32, (int?)(YdbValue)valueInt32);
            Assert.Equal(valueUint32, (uint?)(YdbValue)valueUint32);
            Assert.Equal(valueInt64, (long?)(YdbValue)valueInt64);
            Assert.Equal(valueUint64, (ulong?)(YdbValue)valueUint64);
            Assert.Equal(valueFloat, (float?)(YdbValue)valueFloat);
            Assert.Equal(valueDouble, (double?)(YdbValue)valueDouble);

            // TODO make optional string types
        }

        [Fact]
        public void OptionalType()
        {
            var value = YdbValue.MakeTuple(new YdbValue[] {
                YdbValue.MakeEmptyOptional(YdbTypeId.Int32),
                YdbValue.MakeEmptyOptional(YdbTypeId.String),
                YdbValue.MakeOptional(YdbValue.MakeUtf8("test")),
                YdbValue.MakeOptional(
                    YdbValue.MakeOptional(YdbValue.MakeInt32(17))
                ),
                YdbValue.MakeOptionalInt32(123),
                (YdbValue)(int?)321,
                YdbValue.MakeOptionalInt32(null),
                (YdbValue)(int?)null,
            });

            var elements = value.GetTuple();
            Assert.Equal(8, elements.Count);

            Assert.Null(elements[0].GetOptional());
            Assert.Null(elements[1].GetOptional());

            Assert.Null(elements[0].GetOptionalInt32());
            Assert.Null(elements[1].GetOptionalString());

            Assert.Null((int?)elements[0]);
            Assert.Null((string?)elements[1]);

            Assert.Equal("test", (string)elements[2]!);

            Assert.Equal(17, (int?)elements[3].GetOptional()!);

            Assert.Equal(123, (int?)elements[4]);
            Assert.Equal(321, (int?)elements[5]);
            Assert.Null((int?)elements[6]);
            Assert.Null((int?)elements[7]);
        }

        [Fact]
        public void ListType()
        {
            var value = YdbValue.MakeTuple(new YdbValue[] {
                YdbValue.MakeEmptyList(YdbTypeId.Int32),
                YdbValue.MakeList(new [] { YdbValue.MakeUtf8("one"), YdbValue.MakeUtf8("two") }),
            });

            var elements = value.GetTuple();
            Assert.Equal(2, elements.Count);

            Assert.Equal(0, elements[0].GetList().Count);
            Assert.Equal(new[] { "one", "two" }, elements[1].GetList().Select(v => (string)v!));
        }

        [Fact]
        public void TupleType()
        {
            var value = YdbValue.MakeTuple(new YdbValue[] {
                YdbValue.MakeTuple(new YdbValue[] { }),
            });

            var elements = value.GetTuple();
            Assert.Equal(1, elements.Count);

            Assert.Equal(0, elements[0].GetTuple().Count);
        }

        [Fact]
        public void StructType()
        {
            var value = YdbValue.MakeTuple(new YdbValue[] {
                YdbValue.MakeStruct(new Dictionary<string, YdbValue>()),
                YdbValue.MakeStruct(new Dictionary<string, YdbValue>() {
                    { "Member1", YdbValue.MakeInt64(10) },
                    { "Member2", YdbValue.MakeUtf8("ten") },
                })
            });

            var elements = value.GetTuple();
            Assert.Equal(2, elements.Count);

            Assert.Equal(0, elements[0].GetStruct().Count);

            var s = elements[1].GetStruct();
            Assert.Equal(2, s.Count);
            Assert.Equal(10, (long)s["Member1"]);
            Assert.Equal("ten", (string)s["Member2"]!);
        }
    }
}
