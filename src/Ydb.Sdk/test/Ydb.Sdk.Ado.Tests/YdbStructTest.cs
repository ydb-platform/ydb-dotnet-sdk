using System.Data;
using Xunit;
using Ydb.Sdk.Ado.YdbType;

namespace Ydb.Sdk.Ado.Tests;

public class YdbStructTest : TestBase
{
    [Theory]
    [InlineData(true, 0, 1, 2, 3, 4)]
    [InlineData(true, 4, 0, 1, 2, 3)]
    [InlineData(true, 4, 3, 0, 1, 2)]
    [InlineData(true, 4, 3, 2, 0, 1)]
    [InlineData(true, 4, 3, 2, 1, 0)]
    [InlineData(true, 4, 1, 3, 0, 2)]
    [InlineData(false, 4, 3, 2)]
    [InlineData(false, 2, 3, 4)]
    public Task SetAllTypes_WhenNullOrNotNull_SuccessfullyRoundTrips(bool nullable, params int[] indexes) =>
        RunTestWithTemporaryTable(AllTypesTable(nullable: nullable), $"AllTypesTable_Nullable_{Guid.NewGuid()}",
            async (ydbConnection, tableName) =>
            {
                var ydbStructs = new List<YdbStruct>
                {
                    new()
                    {
                        new YdbParameter { ParameterName = "Int32Column", DbType = DbType.Int32, Value = 1 },
                        new YdbParameter { ParameterName = "BoolColumn", DbType = DbType.Boolean },
                        new YdbParameter { ParameterName = "Int64Column", DbType = DbType.Int64 },
                        new YdbParameter { ParameterName = "Int16Column", DbType = DbType.Int16 },
                        new YdbParameter { ParameterName = "Int8Column", DbType = DbType.SByte },
                        new YdbParameter { ParameterName = "FloatColumn", DbType = DbType.Single },
                        new YdbParameter { ParameterName = "DoubleColumn", DbType = DbType.Double },
                        new YdbParameter { ParameterName = "DefaultDecimalColumn", DbType = DbType.Decimal },
                        new YdbParameter
                        {
                            ParameterName = "CustomDecimalColumn", DbType = DbType.Decimal, Precision = 35, Scale = 5
                        },
                        new YdbParameter { ParameterName = "Uint8Column", DbType = DbType.Byte },
                        new YdbParameter { ParameterName = "Uint16Column", DbType = DbType.UInt16 },
                        new YdbParameter { ParameterName = "Uint32Column", DbType = DbType.UInt32 },
                        new YdbParameter { ParameterName = "Uint64Column", DbType = DbType.UInt64 },
                        new YdbParameter { ParameterName = "TextColumn", DbType = DbType.String },
                        new YdbParameter { ParameterName = "BytesColumn", DbType = DbType.Binary },
                        new YdbParameter { ParameterName = "YsonColumn", YdbDbType = YdbDbType.Yson },
                        new YdbParameter { ParameterName = "JsonColumn", YdbDbType = YdbDbType.Json },
                        new YdbParameter { ParameterName = "JsonDocumentColumn", YdbDbType = YdbDbType.JsonDocument },
                        new YdbParameter { ParameterName = "DateColumn", DbType = DbType.Date },
                        new YdbParameter { ParameterName = "DatetimeColumn", DbType = DbType.DateTime },
                        new YdbParameter { ParameterName = "TimestampColumn", DbType = DbType.DateTime2 },
                        new YdbParameter { ParameterName = "IntervalColumn", YdbDbType = YdbDbType.Interval },
                        new YdbParameter { ParameterName = "Date32Column", YdbDbType = YdbDbType.Date32 },
                        new YdbParameter { ParameterName = "Datetime64Column", YdbDbType = YdbDbType.Datetime64 },
                        new YdbParameter { ParameterName = "Timestamp64Column", YdbDbType = YdbDbType.Timestamp64 },
                        new YdbParameter { ParameterName = "Interval64Column", YdbDbType = YdbDbType.Interval64 }
                    },
                    new()
                    {
                        { "Int32Column", 2, YdbDbType.Int32 },
                        { "BoolColumn", null, YdbDbType.Bool },
                        { "Int64Column", null, YdbDbType.Int64 },
                        { "Int16Column", null, YdbDbType.Int16 },
                        { "Int8Column", null, YdbDbType.Int8 },
                        { "FloatColumn", null, YdbDbType.Float },
                        { "DoubleColumn", null, YdbDbType.Double },
                        { "DefaultDecimalColumn", null, YdbDbType.Decimal },
                        { "CustomDecimalColumn", null, YdbDbType.Decimal, 35, 5 },
                        { "Uint8Column", null, YdbDbType.Uint8 },
                        { "Uint16Column", null, YdbDbType.Uint16 },
                        { "Uint32Column", null, YdbDbType.Uint32 },
                        { "Uint64Column", null, YdbDbType.Uint64 },
                        { "TextColumn", null, YdbDbType.Text },
                        { "BytesColumn", null, YdbDbType.Bytes },
                        { "YsonColumn", null, YdbDbType.Yson },
                        { "JsonColumn", null, YdbDbType.Json },
                        { "JsonDocumentColumn", null, YdbDbType.JsonDocument },
                        { "DateColumn", null, YdbDbType.Date },
                        { "DatetimeColumn", null, YdbDbType.Datetime },
                        { "TimestampColumn", null, YdbDbType.Timestamp },
                        { "IntervalColumn", null, YdbDbType.Interval },
                        { "Date32Column", null, YdbDbType.Date32 },
                        { "Datetime64Column", null, YdbDbType.Datetime64 },
                        { "Timestamp64Column", null, YdbDbType.Timestamp64 },
                        { "Interval64Column", null, YdbDbType.Interval64 }
                    },
                    new()
                    {
                        new YdbParameter("Int32Column", YdbDbType.Int32, 3),
                        new YdbParameter("BoolColumn", YdbDbType.Bool, true),
                        new YdbParameter("Int64Column", YdbDbType.Int64, 1),
                        new YdbParameter("Int16Column", YdbDbType.Int16, (short)1),
                        new YdbParameter("Int8Column", YdbDbType.Int8, (sbyte)1),
                        new YdbParameter("FloatColumn", YdbDbType.Float, 1.0f),
                        new YdbParameter("DoubleColumn", YdbDbType.Double, 1.0),
                        new YdbParameter("DefaultDecimalColumn", YdbDbType.Decimal, 1m),
                        new YdbParameter("CustomDecimalColumn", YdbDbType.Decimal, 1m) { Precision = 35, Scale = 5 },
                        new YdbParameter("Uint8Column", YdbDbType.Uint8, (byte)1),
                        new YdbParameter("Uint16Column", YdbDbType.Uint16, (ushort)1),
                        new YdbParameter("Uint32Column", YdbDbType.Uint32, (uint)1),
                        new YdbParameter("Uint64Column", YdbDbType.Uint64, (ulong)1),
                        new YdbParameter("TextColumn", YdbDbType.Text, string.Empty),
                        new YdbParameter("BytesColumn", YdbDbType.Bytes, Array.Empty<byte>()),
                        new YdbParameter("YsonColumn", YdbDbType.Yson, "{a=1u}"u8.ToArray()),
                        new YdbParameter("JsonColumn", YdbDbType.Json, "{}"),
                        new YdbParameter("JsonDocumentColumn", YdbDbType.JsonDocument, "{}"),
                        new YdbParameter("DateColumn", YdbDbType.Date, DateTime.UnixEpoch),
                        new YdbParameter("DatetimeColumn", YdbDbType.Datetime, DateTime.UnixEpoch),
                        new YdbParameter("TimestampColumn", YdbDbType.Timestamp, DateTime.UnixEpoch),
                        new YdbParameter("IntervalColumn", YdbDbType.Interval, TimeSpan.Zero),
                        new YdbParameter("Date32Column", YdbDbType.Date32, DateTime.MinValue),
                        new YdbParameter("Datetime64Column", YdbDbType.Datetime64, DateTime.MinValue),
                        new YdbParameter("Timestamp64Column", YdbDbType.Timestamp64, DateTime.MinValue),
                        new YdbParameter("Interval64Column", YdbDbType.Interval64,
                            TimeSpan.FromMilliseconds(TimeSpan.MinValue.Milliseconds))
                    },
                    new()
                    {
                        { "Int32Column", 4, YdbDbType.Int32 },
                        { "BoolColumn", true, YdbDbType.Bool },
                        { "Int64Column", 1, YdbDbType.Int64 },
                        { "Int16Column", (short)1, YdbDbType.Int16 },
                        { "Int8Column", (sbyte)1, YdbDbType.Int8 },
                        { "FloatColumn", 1.0f, YdbDbType.Float },
                        { "DoubleColumn", 1.0, YdbDbType.Double },
                        { "DefaultDecimalColumn", 1m, YdbDbType.Decimal },
                        { "CustomDecimalColumn", 1m, YdbDbType.Decimal, 35, 5 },
                        { "Uint8Column", (byte)1, YdbDbType.Uint8 },
                        { "Uint16Column", (ushort)1, YdbDbType.Uint16 },
                        { "Uint32Column", (uint)1, YdbDbType.Uint32 },
                        { "Uint64Column", (ulong)1, YdbDbType.Uint64 },
                        { "TextColumn", string.Empty, YdbDbType.Text },
                        { "BytesColumn", Array.Empty<byte>(), YdbDbType.Bytes },
                        { "YsonColumn", "{a=1u}"u8.ToArray(), YdbDbType.Yson },
                        { "JsonColumn", "{}", YdbDbType.Json },
                        { "JsonDocumentColumn", "{}", YdbDbType.JsonDocument },
                        { "DateColumn", DateOnly.FromDateTime(DateTime.UnixEpoch), YdbDbType.Date },
                        { "DatetimeColumn", DateTime.UnixEpoch, YdbDbType.Datetime },
                        { "TimestampColumn", DateTime.UnixEpoch, YdbDbType.Timestamp },
                        { "IntervalColumn", TimeSpan.Zero, YdbDbType.Interval },
                        { "Date32Column", DateTime.MinValue, YdbDbType.Date32 },
                        { "Datetime64Column", DateTime.MinValue, YdbDbType.Datetime64 },
                        { "Timestamp64Column", DateTime.MinValue, YdbDbType.Timestamp64 },
                        {
                            "Interval64Column", TimeSpan.FromMilliseconds(TimeSpan.MinValue.Milliseconds),
                            YdbDbType.Interval64
                        }
                    },
                    new()
                    {
                        { "Int32Column", 5 },
                        { "BoolColumn", true },
                        { "Int64Column", 1L },
                        { "Int16Column", (short)1 },
                        { "Int8Column", (sbyte)1 },
                        { "FloatColumn", 1.0f },
                        { "DoubleColumn", 1.0 },
                        { "DefaultDecimalColumn", 1m },
                        { "CustomDecimalColumn", 1m, YdbDbType.Decimal, 35, 5 },
                        { "Uint8Column", (byte)1 },
                        { "Uint16Column", (ushort)1 },
                        { "Uint32Column", (uint)1 },
                        { "Uint64Column", (ulong)1 },
                        { "TextColumn", string.Empty },
                        { "BytesColumn", Array.Empty<byte>() },
                        { "YsonColumn", "{a=1u}"u8.ToArray(), YdbDbType.Yson },
                        { "JsonColumn", "{}", YdbDbType.Json },
                        { "JsonDocumentColumn", "{}", YdbDbType.JsonDocument },
                        { "DateColumn", DateOnly.FromDateTime(DateTime.UnixEpoch) },
                        { "DatetimeColumn", DateTime.UnixEpoch, YdbDbType.Datetime },
                        { "TimestampColumn", DateTime.UnixEpoch, YdbDbType.Timestamp },
                        { "IntervalColumn", TimeSpan.Zero },
                        { "Date32Column", DateTime.MinValue, YdbDbType.Date32 },
                        { "Datetime64Column", DateTime.MinValue, YdbDbType.Datetime64 },
                        { "Timestamp64Column", DateTime.MinValue, YdbDbType.Timestamp64 },
                        {
                            "Interval64Column", TimeSpan.FromMilliseconds(TimeSpan.MinValue.Milliseconds),
                            YdbDbType.Interval64
                        }
                    }
                };
                await new YdbCommand($"INSERT INTO `{tableName}` SELECT * FROM AS_TABLE($values)", ydbConnection)
                        { Parameters = { new YdbParameter("values", indexes.Select(i => ydbStructs[i]).ToList()) } }
                    .ExecuteNonQueryAsync();

                var ydbDataReader = await new YdbCommand(SelectAllTypesTable(tableName), ydbConnection)
                    .ExecuteReaderAsync();

                if (nullable)
                {
                    for (var it = 1; it <= 2; it++)
                    {
                        Assert.True(await ydbDataReader.ReadAsync());
                        Assert.Equal(26, ydbDataReader.FieldCount);
                        Assert.Equal(it, ydbDataReader.GetInt32(0));
                        for (var i = 1; i < 26; i++)
                        {
                            Assert.True(ydbDataReader.IsDBNull(i));
                            Assert.Equal(DBNull.Value, ydbDataReader.GetValue(i));
                        }
                    }
                }

                for (var it = 3; it <= 5; it++)
                {
                    Assert.True(await ydbDataReader.ReadAsync());
                    Assert.Equal(26, ydbDataReader.FieldCount);
                    Assert.Equal(it, ydbDataReader.GetInt32(0));
                    Assert.True(ydbDataReader.GetBoolean(1));
                    Assert.Equal(1, ydbDataReader.GetInt64(2));
                    Assert.Equal(1, ydbDataReader.GetInt16(3));
                    Assert.Equal(1, ydbDataReader.GetSByte(4));
                    Assert.Equal(1.0, ydbDataReader.GetFloat(5));
                    Assert.Equal(1.0, ydbDataReader.GetDouble(6));
                    Assert.Equal(1.000000000m, ydbDataReader.GetDecimal(7));
                    Assert.Equal(1.00000m, ydbDataReader.GetDecimal(8));
                    Assert.Equal(1, ydbDataReader.GetByte(9));
                    Assert.Equal(1, ydbDataReader.GetUint16(10));
                    Assert.Equal((uint)1, ydbDataReader.GetUint32(11));
                    Assert.Equal((ulong)1, ydbDataReader.GetUint64(12));
                    Assert.Equal(string.Empty, ydbDataReader.GetString(13));
                    Assert.Equal(Array.Empty<byte>(), ydbDataReader.GetBytes(14));
                    Assert.Equal(DateTime.UnixEpoch, ydbDataReader.GetDateTime(15));
                    Assert.Equal(DateTime.UnixEpoch, ydbDataReader.GetDateTime(16));
                    Assert.Equal(DateTime.UnixEpoch, ydbDataReader.GetDateTime(17));
                    Assert.Equal(TimeSpan.Zero, ydbDataReader.GetInterval(18));
                    Assert.Equal("{}", ydbDataReader.GetString(19));
                    Assert.Equal("{}", ydbDataReader.GetString(20));
                    Assert.Equal("{a=1u}"u8.ToArray(), ydbDataReader.GetBytes(21));
                    Assert.Equal(DateTime.MinValue, ydbDataReader.GetDateTime(22));
                    Assert.Equal(DateTime.MinValue, ydbDataReader.GetDateTime(23));
                    Assert.Equal(DateTime.MinValue, ydbDataReader.GetDateTime(24));
                    Assert.Equal(TimeSpan.FromMilliseconds(TimeSpan.MinValue.Milliseconds),
                        ydbDataReader.GetInterval(25));
                }

                Assert.False(ydbDataReader.Read());
            });

    [Fact]
    public Task InsertUpdateDelete_15000_TestEntities_Works() => RunTestWithTemporaryTable(
        """
        CREATE TABLE `{0}` (
            id Uint64,
            name Text NOT NULL,
            is_active Bool NOT NULL,
            updated_at Timestamp64 NOT NULL,
            PRIMARY KEY (id)
        );
        """, $"entity_table_{Guid.NewGuid()}", async (ydbConnection, tableName) =>
        {
            const ulong batchSize = 15_000ul;

            var insertNow = DateTime.UtcNow;
            var ydbStructsForInsert = new List<YdbStruct>();
            for (ulong i = 0; i < batchSize; i++)
            {
                ydbStructsForInsert.Add(new YdbStruct
                {
                    { "id", i },
                    { "name", $"name-{i}" },
                    { "is_active", true },
                    { "updated_at", insertNow }
                });
            }

            await new YdbCommand($"INSERT INTO `{tableName}` SELECT * FROM AS_TABLE($values)", ydbConnection)
                { Parameters = { new YdbParameter("values", ydbStructsForInsert) } }.ExecuteNonQueryAsync();
            Assert.Equal(batchSize, await new YdbCommand($"SELECT COUNT(*) FROM `{tableName}`", ydbConnection)
                .ExecuteScalarAsync());

            var ydbStructsForUpdate = new List<YdbStruct>();
            var updateNow = DateTime.UtcNow;
            for (ulong i = 0; i < batchSize; i++)
            {
                ydbStructsForUpdate.Add(new YdbStruct
                {
                    { "id", i },
                    { "name", $"name-{i}" },
                    { "is_active", false },
                    { "updated_at", updateNow }
                });
            }

            await new YdbCommand($"UPDATE `{tableName}` ON SELECT * FROM AS_TABLE($values)", ydbConnection)
                { Parameters = { new YdbParameter("values", ydbStructsForUpdate) } }.ExecuteNonQueryAsync();
            Assert.Equal(batchSize, await new YdbCommand($"SELECT COUNT(*) FROM `{tableName}`" +
                                                         " WHERE is_active = FALSE AND updated_at = @updated_at",
                    ydbConnection) { Parameters = { new YdbParameter("updated_at", YdbDbType.Timestamp64, updateNow) } }
                .ExecuteScalarAsync());

            var ydbStructsForUpsert = new List<YdbStruct>();
            var upsertNow = DateTime.UtcNow;
            for (ulong i = 0; i < 2 * batchSize; i++)
            {
                ydbStructsForUpsert.Add(new YdbStruct
                {
                    { "id", i },
                    { "name", $"name-{i}" },
                    { "is_active", true },
                    { "updated_at", upsertNow }
                });
            }

            await new YdbCommand($"UPSERT INTO `{tableName}` SELECT * FROM AS_TABLE($values)", ydbConnection)
                { Parameters = { new YdbParameter("values", ydbStructsForUpsert) } }.ExecuteNonQueryAsync();
            Assert.Equal(2 * batchSize, await new YdbCommand($"SELECT COUNT(*) FROM `{tableName}`" +
                                                             " WHERE is_active = TRUE AND updated_at = @updated_at",
                    ydbConnection) { Parameters = { new YdbParameter("updated_at", YdbDbType.Timestamp64, upsertNow) } }
                .ExecuteScalarAsync());

            await new YdbCommand($"DELETE FROM `{tableName}` ON SELECT * FROM AS_TABLE($values)", ydbConnection)
                { Parameters = { new YdbParameter("values", ydbStructsForUpsert) } }.ExecuteNonQueryAsync();
            Assert.Equal(0ul, await new YdbCommand($"SELECT COUNT(*) FROM `{tableName}`", ydbConnection)
                .ExecuteScalarAsync());
        });

    [Fact]
    public void YdbParameter_WhenYdbStructListIsEmpty_ThrowsInvalidOperationException() => Assert.Equal(
        "Collection of 'YdbStruct' can't be empty.",
        Assert.Throws<InvalidOperationException>(() => new YdbParameter("name", new List<YdbStruct>()).TypedValue).Message);
    
    [Fact]
    public void YdbParameter_WhenSchemaMemberCountDiffers_ThrowsInvalidOperationException() => Assert.Equal(
        "YdbStruct schema mismatch: expected 2 members, actual 3.",
        Assert.Throws<InvalidOperationException>(() => new YdbParameter("name", new List<YdbStruct>
        {
            new()
            {
                { "id", 1 },
                { "name", "name" },
            },
            new()
            {
                { "id", 2 },
                { "name", "name" },
                { "surname", "surname" }
            }
        }).TypedValue).Message);
    
    [Fact]
    public void YdbParameter_WhenSchemaMembersDiffer_ThrowsInvalidOperationException() => Assert.Equal(
        "YdbStruct schema mismatch: expected member '{ \"name\": \"name\", \"type\": { \"typeId\": \"UTF8\" } }', " +
        "actual member '{ \"name\": \"surname\", \"type\": { \"typeId\": \"UTF8\" } }'.",
        Assert.Throws<InvalidOperationException>(() => new YdbParameter("name", new List<YdbStruct>
        {
            new()
            {
                { "id", 1 },
                { "name", "name" },
            },
            new()
            {
                { "id", 2 },
                { "surname", "surname" }
            }
        }).TypedValue).Message);
}
