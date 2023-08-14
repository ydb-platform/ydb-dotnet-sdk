#if NETCOREAPP3_1
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
#endif

using System.Text;
using Xunit;
using Xunit.Abstractions;
using Ydb.Sdk.Table;

namespace Ydb.Sdk.Value.Tests
{
    [Trait("Category", "Integration")]
    public class TestBasicIntegration : IDisposable
    {
        private readonly ITestOutputHelper _output;

        private readonly Driver _driver;
        private readonly TableClient _tableClient;

        public TestBasicIntegration(ITestOutputHelper output)
        {
            _output = output;
            var driverConfig = new DriverConfig(
                endpoint: "grpc://localhost:2136",
                database: "/local"
            );

            _driver = new Driver(driverConfig);
            _driver.Initialize().Wait();

            _tableClient = new TableClient(_driver);
        }

        public void Dispose()
        {
            _driver.Dispose();
            GC.SuppressFinalize(this);
        }

        [Fact]
        public async Task Simple()
        {
            var query = @"SELECT 1;";

            var response = await _tableClient.SessionExec(async session =>
                await session.ExecuteDataQuery(
                    query: query,
                    txControl: TxControl.BeginSerializableRW().Commit()
                )
            );
            Assert.NotNull(response);
            Assert.True(response.Status.IsSuccess);
            var queryResponse = (ExecuteDataQueryResponse)response;
            var row = queryResponse.Result.ResultSets[0].Rows[0];
            Assert.Equal(1, row[0].GetInt32());
        }

        private async Task<YdbValue> SelectPrimiriveValue(YdbValue value)
        {
            var response = await _tableClient.SessionExec(async session =>
                await session.ExecuteDataQuery(
                    query: "SELECT $value;",
                    txControl: TxControl.BeginSerializableRW().Commit(),
                    parameters: new Dictionary<string, YdbValue> { { "$value", value } }
                )
            );
            Assert.NotNull(response);
            response.Status.EnsureSuccess();
            Assert.True(response.Status.IsSuccess);
            var queryResponse = (ExecuteDataQueryResponse)response;
            Assert.True(queryResponse.Result.ResultSets.Count > 0);
            Assert.True(queryResponse.Result.ResultSets[0].Rows.Count > 0);
            var row = queryResponse.Result.ResultSets[0].Rows[0];
            return row[0];
        }

        [Fact]
        public async Task PrimitiveTypes()
        {
            var valueBool = true;
            var resultBool = await SelectPrimiriveValue(YdbValue.MakeBool(valueBool));
            Assert.Equal(resultBool.GetBool(), valueBool);

            var valueInt8 = (sbyte)-1;
            var resultInt8 = await SelectPrimiriveValue(YdbValue.MakeInt8(valueInt8));
            Assert.Equal(resultInt8.GetInt8(), valueInt8);

            var valueUint8 = (byte)200;
            var resultUint8 = await SelectPrimiriveValue(YdbValue.MakeUint8(valueUint8));
            Assert.Equal(resultUint8.GetUint8(), valueUint8);

            var valueInt16 = (short)-200;
            var resultInt16 = await SelectPrimiriveValue(YdbValue.MakeInt16(valueInt16));
            Assert.Equal(resultInt16.GetInt16(), valueInt16);

            var valueUint16 = (ushort)40_000;
            var resultUint16 = await SelectPrimiriveValue(YdbValue.MakeUint16(valueUint16));
            Assert.Equal(resultUint16.GetUint16(), valueUint16);

            var valueInt32 = -40000;
            var resultInt32 = await SelectPrimiriveValue(YdbValue.MakeInt32(valueInt32));
            Assert.Equal(resultInt32.GetInt32(), valueInt32);

            var valueUint32 = 4_000_000_000;
            var resultUint32 = await SelectPrimiriveValue(YdbValue.MakeUint32(valueUint32));
            Assert.Equal(resultUint32.GetUint32(), valueUint32);

            var valueInt64 = -4_000_000_000;
            var resultInt64 = await SelectPrimiriveValue(YdbValue.MakeInt64(valueInt64));
            Assert.Equal(resultInt64.GetInt64(), valueInt64);

            var valueUint64 = 10_000_000_000ul;
            var resultUint64 = await SelectPrimiriveValue(YdbValue.MakeUint64(valueUint64));
            Assert.Equal(resultUint64.GetUint64(), valueUint64);

            var valueFloat = -1.7f;
            var resultFloat = await SelectPrimiriveValue(YdbValue.MakeFloat(valueFloat));
            Assert.Equal(resultFloat.GetFloat(), valueFloat);

            var valueDouble = 123.45;
            var resultDouble = await SelectPrimiriveValue(YdbValue.MakeDouble(valueDouble));
            Assert.Equal(resultDouble.GetDouble(), valueDouble);

            var valueDate = new DateTime(2021, 08, 21);
            var resultDate = await SelectPrimiriveValue(YdbValue.MakeDate(valueDate));
            Assert.Equal(resultDate.GetDate(), valueDate);

            var valueDatetime = new DateTime(2021, 08, 21, 23, 30, 47);
            var resultDatetime = await SelectPrimiriveValue(YdbValue.MakeDatetime(valueDatetime));
            Assert.Equal(resultDatetime.GetDatetime(), valueDatetime);

            var valueTimestamp = new DateTime(2021, 08, 21, 23, 30, 47, 581);
            var resultTimestamp = await SelectPrimiriveValue(YdbValue.MakeTimestamp(valueTimestamp));
            Assert.Equal(resultTimestamp.GetTimestamp(), valueTimestamp);

            var valueInterval = -new TimeSpan(3, 7, 40, 27, 729);
            var resultInterval = await SelectPrimiriveValue(YdbValue.MakeInterval(valueInterval));
            Assert.Equal(resultInterval.GetInterval(), valueInterval);

            var valueString = Encoding.ASCII.GetBytes("test str");
            var resultString = await SelectPrimiriveValue(YdbValue.MakeString(valueString));
            Assert.Equal(resultString.GetString(), valueString);

            var valueUtf8 = "unicode str";
            var resultUtf8 = await SelectPrimiriveValue(YdbValue.MakeUtf8(valueUtf8));
            Assert.Equal(resultUtf8.GetUtf8(), valueUtf8);

            var valueYson = Encoding.ASCII.GetBytes("{type=\"yson\"}");
            var resultYson = await SelectPrimiriveValue(YdbValue.MakeYson(valueYson));
            Assert.Equal(resultYson.GetYson(), valueYson);

            var valueJson = "{\"type\": \"json\"}";
            var resultJson = await SelectPrimiriveValue(YdbValue.MakeJson(valueJson));
            Assert.Equal(resultJson.GetJson(), valueJson);

            var valueJsonDocument = "{\"type\":\"jsondoc\"}";
            var resultJsonDocument = await SelectPrimiriveValue(YdbValue.MakeJsonDocument(valueJsonDocument));
            Assert.Equal(resultJsonDocument.GetJsonDocument(), valueJsonDocument);
        }
    }
}