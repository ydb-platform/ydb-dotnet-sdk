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
    public class TestBasic
    {
        private readonly ITestOutputHelper _output;

        public TestBasic(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void PrimitiveTypes()
        {
            var value = YdbValue.MakeTuple(new YdbValue[] {
                YdbValue.MakeInt8(-1),
                YdbValue.MakeUint8(200),
                YdbValue.MakeInt16(-200),
                YdbValue.MakeUint16(40_000),
                YdbValue.MakeInt32(-40000),
                YdbValue.MakeUint32(4_000_000_000),
                YdbValue.MakeInt64(-4_000_000_000),
                YdbValue.MakeUint64(10_000_000_000),
                YdbValue.MakeFloat(-1.7f),
                YdbValue.MakeDouble(123.45),
                YdbValue.MakeDate(new DateTime(2021, 08, 21)),
                YdbValue.MakeDatetime(new DateTime(2021, 08, 21, 23, 30, 47)),
                YdbValue.MakeTimestamp(new DateTime(2021, 08, 21, 23, 30, 47, 581)),
                YdbValue.MakeInterval(-new TimeSpan(3, 7, 40, 27, 729)),
                YdbValue.MakeString(Encoding.ASCII.GetBytes("test str")),
                YdbValue.MakeUtf8("unicode str"),
                YdbValue.MakeYson(Encoding.ASCII.GetBytes("{type=\"yson\"}")),
                YdbValue.MakeJson("{\"type\": \"json\"}"),
                YdbValue.MakeJsonDocument("{\"type\": \"jsondoc\"}"),
                YdbValue.MakeBool(true),
                YdbValue.MakeBool(false),
            });

            _output.WriteLine(value.ToString());

            var elements = value.GetTuple();
            Assert.Equal(21, elements.Count);

            Assert.Equal(-1, elements[0].GetInt8());
            Assert.Equal(200, elements[1].GetUint8());
            Assert.Equal(-200, elements[2].GetInt16());
            Assert.Equal(40_000, elements[3].GetUint16());
            Assert.Equal(-40_000, elements[4].GetInt32());
            Assert.Equal(4_000_000_000, elements[5].GetUint32());
            Assert.Equal(-4_000_000_000, elements[6].GetInt64());
            Assert.Equal(10_000_000_000ul, elements[7].GetUint64());
            Assert.Equal(-1.7f, elements[8].GetFloat());
            Assert.Equal(123.45, elements[9].GetDouble());
            Assert.Equal(new DateTime(2021, 08, 21), elements[10].GetDate());
            Assert.Equal(new DateTime(2021, 08, 21, 23, 30, 47), elements[11].GetDatetime());
            Assert.Equal(new DateTime(2021, 08, 21, 23, 30, 47, 581), elements[12].GetTimestamp());
            Assert.Equal(-new TimeSpan(3, 7, 40, 27, 729), elements[13].GetInterval());
            Assert.Equal("test str", Encoding.ASCII.GetString(elements[14].GetString()));
            Assert.Equal("unicode str", elements[15].GetUtf8());
            Assert.Equal("{type=\"yson\"}", Encoding.ASCII.GetString(elements[16].GetYson()));
            Assert.Equal("{\"type\": \"json\"}", elements[17].GetJson());
            Assert.Equal("{\"type\": \"jsondoc\"}", elements[18].GetJsonDocument());
            Assert.True(elements[19].GetBool());
            Assert.False(elements[20].GetBool());

            Assert.Equal(-1, (sbyte)elements[0]);
            Assert.Equal(200u, (byte)elements[1]);
            Assert.Equal(-200, (short)elements[2]);
            Assert.Equal(40_000, (ushort) elements[3]);
            Assert.Equal(-40_000, (int) elements[4]);
            Assert.Equal(4_000_000_000, (uint)elements[5]);
            Assert.Equal(-4_000_000_000, (long)elements[6]);
            Assert.Equal(10_000_000_000ul, (ulong)elements[7]);
            Assert.Equal(-1.7f, (float)elements[8]);
            Assert.Equal(123.45, (double)elements[9]);
            Assert.Equal(new DateTime(2021, 08, 21), (DateTime)elements[10]);
            Assert.Equal(new DateTime(2021, 08, 21, 23, 30, 47), (DateTime)elements[11]);
            Assert.Equal(new DateTime(2021, 08, 21, 23, 30, 47, 581), (DateTime)elements[12]);
            Assert.Equal(-new TimeSpan(3, 7, 40, 27, 729), (TimeSpan)elements[13]);
            Assert.Equal("test str", Encoding.ASCII.GetString((byte[])elements[14]!));
            Assert.Equal("unicode str", (string)elements[15]!);
            Assert.Equal("{type=\"yson\"}", Encoding.ASCII.GetString((byte[])elements[16]!));
            Assert.Equal("{\"type\": \"json\"}", (string)elements[17]!);
            Assert.Equal("{\"type\": \"jsondoc\"}", (string)elements[18]!);
            Assert.True((bool)elements[19]);
            Assert.False((bool)elements[20]);
        }

        [Fact]
        public void OptimalPrimitiveTypes()
        {
            var value = YdbValue.MakeTuple(new YdbValue[] {
                (YdbValue)(sbyte?)-1,
                (YdbValue)(byte?)200,
                (YdbValue)(short?)-200,
                (YdbValue)(ushort?)40_000,
                (YdbValue)(int?)-40000,
                (YdbValue)(uint?)4_000_000_000,
                (YdbValue)(long?)-4_000_000_000,
                (YdbValue)(ulong?)10_000_000_000,
                (YdbValue)(float?)-1.7f,
                (YdbValue)(double?)123.45,
                YdbValue.MakeOptionalDate(new DateTime(2021, 08, 21)),
                YdbValue.MakeOptionalDatetime(new DateTime(2021, 08, 21, 23, 30, 47)),
                YdbValue.MakeOptionalTimestamp(new DateTime(2021, 08, 21, 23, 30, 47, 581)),
                YdbValue.MakeOptionalInterval(-new TimeSpan(3, 7, 40, 27, 729)),
                (YdbValue)(bool?)true,
                (YdbValue)(bool?)false,
            });

            _output.WriteLine(value.ToString());

            var elements = value.GetTuple();
            Assert.Equal(16, elements.Count);

            Assert.Equal((sbyte?)-1, elements[0].GetOptionalInt8());
            Assert.Equal((byte?)200, elements[1].GetOptionalUint8());
            Assert.Equal((short?)-200, elements[2].GetOptionalInt16());
            Assert.Equal((ushort?)40_000, elements[3].GetOptionalUint16());
            Assert.Equal(-40_000, elements[4].GetOptionalInt32());
            Assert.Equal(4_000_000_000, elements[5].GetOptionalUint32());
            Assert.Equal(-4_000_000_000, elements[6].GetOptionalInt64());
            Assert.Equal(10_000_000_000ul, elements[7].GetOptionalUint64());
            Assert.Equal(-1.7f, elements[8].GetOptionalFloat());
            Assert.Equal(123.45, elements[9].GetOptionalDouble());
            Assert.Equal(new DateTime(2021, 08, 21), elements[10].GetOptionalDate());
            Assert.Equal(new DateTime(2021, 08, 21, 23, 30, 47), elements[11].GetOptionalDatetime());
            Assert.Equal(new DateTime(2021, 08, 21, 23, 30, 47, 581), elements[12].GetOptionalTimestamp());
            Assert.Equal(-new TimeSpan(3, 7, 40, 27, 729), elements[13].GetOptionalInterval());
            Assert.Equal(true, elements[14].GetOptionalBool());
            Assert.Equal(false, elements[15].GetOptionalBool());

            Assert.Equal((sbyte?)-1, (sbyte?)elements[0]);
            Assert.Equal((byte?)200u, (byte?)elements[1]);
            Assert.Equal((short?)-200, (short?)elements[2]);
            Assert.Equal((ushort?)40_000, (ushort?) elements[3]);
            Assert.Equal(-40_000, (int?) elements[4]);
            Assert.Equal(4_000_000_000, (uint?)elements[5]);
            Assert.Equal(-4_000_000_000, (long?)elements[6]);
            Assert.Equal(10_000_000_000ul, (ulong?)elements[7]);
            Assert.Equal(-1.7f, (float?)elements[8]);
            Assert.Equal(123.45, (double?)elements[9]);
            Assert.Equal(new DateTime(2021, 08, 21), (DateTime?)elements[10]);
            Assert.Equal(new DateTime(2021, 08, 21, 23, 30, 47), (DateTime?)elements[11]);
            Assert.Equal(new DateTime(2021, 08, 21, 23, 30, 47, 581), (DateTime?)elements[12]);
            Assert.Equal(-new TimeSpan(3, 7, 40, 27, 729), (TimeSpan?)elements[13]);
            Assert.True((bool?)elements[14]);
            Assert.False((bool?)elements[15]);
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
            });

            var elements = value.GetTuple();
            Assert.Equal(4, elements.Count);

            Assert.Null(elements[0].GetOptional());
            Assert.Null(elements[1].GetOptional());

            Assert.Null(elements[0].GetOptionalInt32());
            Assert.Null(elements[1].GetOptionalString());

            Assert.Null((int?)elements[0]);
            Assert.Null((string?)elements[1]);

            Assert.Equal("test", (string)elements[2]!);

            Assert.Equal(17, (int?)elements[3].GetOptional()!);
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
