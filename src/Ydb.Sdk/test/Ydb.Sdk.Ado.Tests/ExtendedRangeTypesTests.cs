using Xunit;
using Ydb.Sdk.Ado.YdbType;

namespace Ydb.Sdk.Ado.Tests;

/// <summary>
/// Tests for reading and writing raw values for Date32, Datetime64, Timestamp64, and Interval64 types
/// that can be outside of DateTime/TimeSpan supported range.
/// </summary>
public class ExtendedRangeTypesTests : TestBase
{
    private const string TableSchema = """
                                       CREATE TABLE `{0}` (
                                           Id Int32 NOT NULL,
                                           Date32Column Date32,
                                           Datetime64Column DateTime64,
                                           Timestamp64Column Timestamp64,
                                           Interval64Column Interval64,
                                           PRIMARY KEY (Id)
                                       )
                                       """;

    [Fact]
    public async Task Date32_ReadRawValue_WithGetInt32()
    {
        const string tableName = "Date32RawValueTest";
        await RunTestWithTemporaryTable(TableSchema, tableName, async (connection, table) =>
        {
            // Insert a Date32 value using raw int (days from Unix epoch)
            // Use a date outside DateTime supported range
            const int rawDaysValue = -100000; // Represents a date around year 1696

            await new YdbCommand($"INSERT INTO `{table}` (Id, Date32Column) VALUES (@Id, CAST(@Date32Value AS Date32))",
                connection)
            {
                Parameters =
                {
                    new YdbParameter("@Id", 1),
                    new YdbParameter("@Date32Value", YdbDbType.Date32, rawDaysValue)
                }
            }.ExecuteNonQueryAsync();

            // Read the raw value using GetInt32
            var reader = await new YdbCommand($"SELECT Date32Column FROM `{table}` WHERE Id = 1", connection)
                .ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            Assert.False(reader.IsDBNull(0));
            var readValue = reader.GetInt32(0);
            Assert.Equal(rawDaysValue, readValue);
        });
    }

    [Fact]
    public async Task Date32_ReadRawValue_PositiveValue()
    {
        const string tableName = "Date32PositiveRawValueTest";
        await RunTestWithTemporaryTable(TableSchema, tableName, async (connection, table) =>
        {
            // Insert a Date32 value using raw int (days from Unix epoch)
            // Use a future date outside DateTime supported range
            const int rawDaysValue = 100000; // Represents a date around year 2243

            await new YdbCommand($"INSERT INTO `{table}` (Id, Date32Column) VALUES (@Id, CAST(@Date32Value AS Date32))",
                connection)
            {
                Parameters =
                {
                    new YdbParameter("@Id", 1),
                    new YdbParameter("@Date32Value", YdbDbType.Date32, rawDaysValue)
                }
            }.ExecuteNonQueryAsync();

            // Read the raw value using GetInt32
            var reader = await new YdbCommand($"SELECT Date32Column FROM `{table}` WHERE Id = 1", connection)
                .ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            var readValue = reader.GetInt32(0);
            Assert.Equal(rawDaysValue, readValue);
        });
    }

    [Fact]
    public async Task Datetime64_ReadRawValue_WithGetInt64()
    {
        const string tableName = "Datetime64RawValueTest";
        await RunTestWithTemporaryTable(TableSchema, tableName, async (connection, table) =>
        {
            // Insert a Datetime64 value using raw long (seconds from Unix epoch)
            // Use a datetime outside DateTime supported range
            const long rawSecondsValue = -10000000000L; // Represents a date around year 1653

            await new YdbCommand(
                $"INSERT INTO `{table}` (Id, Datetime64Column) VALUES (@Id, CAST(@Datetime64Value AS Datetime64))",
                connection)
            {
                Parameters =
                {
                    new YdbParameter("@Id", 1),
                    new YdbParameter("@Datetime64Value", YdbDbType.Datetime64, rawSecondsValue)
                }
            }.ExecuteNonQueryAsync();

            // Read the raw value using GetInt64
            var reader = await new YdbCommand($"SELECT Datetime64Column FROM `{table}` WHERE Id = 1", connection)
                .ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            Assert.False(reader.IsDBNull(0));
            var readValue = reader.GetInt64(0);
            Assert.Equal(rawSecondsValue, readValue);
        });
    }

    [Fact]
    public async Task Timestamp64_ReadRawValue_WithGetInt64()
    {
        const string tableName = "Timestamp64RawValueTest";
        await RunTestWithTemporaryTable(TableSchema, tableName, async (connection, table) =>
        {
            // Insert a Timestamp64 value using raw long (microseconds from Unix epoch)
            const long rawMicrosecondsValue = -10000000000000L; // Represents a timestamp around year 1653

            await new YdbCommand(
                $"INSERT INTO `{table}` (Id, Timestamp64Column) VALUES (@Id, CAST(@Timestamp64Value AS Timestamp64))",
                connection)
            {
                Parameters =
                {
                    new YdbParameter("@Id", 1),
                    new YdbParameter("@Timestamp64Value", YdbDbType.Timestamp64, rawMicrosecondsValue)
                }
            }.ExecuteNonQueryAsync();

            // Read the raw value using GetInt64
            var reader = await new YdbCommand($"SELECT Timestamp64Column FROM `{table}` WHERE Id = 1", connection)
                .ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            Assert.False(reader.IsDBNull(0));
            var readValue = reader.GetInt64(0);
            Assert.Equal(rawMicrosecondsValue, readValue);
        });
    }

    [Fact]
    public async Task Interval64_ReadRawValue_WithGetInt64()
    {
        const string tableName = "Interval64RawValueTest";
        await RunTestWithTemporaryTable(TableSchema, tableName, async (connection, table) =>
        {
            // Insert an Interval64 value using raw long (microseconds)
            // Use an interval outside TimeSpan supported range
            const long rawMicrosecondsValue = 10000000000000L; // Large interval

            await new YdbCommand(
                $"INSERT INTO `{table}` (Id, Interval64Column) VALUES (@Id, CAST(@Interval64Value AS Interval64))",
                connection)
            {
                Parameters =
                {
                    new YdbParameter("@Id", 1),
                    new YdbParameter("@Interval64Value", YdbDbType.Interval64, rawMicrosecondsValue)
                }
            }.ExecuteNonQueryAsync();

            // Read the raw value using GetInt64
            var reader = await new YdbCommand($"SELECT Interval64Column FROM `{table}` WHERE Id = 1", connection)
                .ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            Assert.False(reader.IsDBNull(0));
            var readValue = reader.GetInt64(0);
            Assert.Equal(rawMicrosecondsValue, readValue);
        });
    }

    [Fact]
    public async Task Date32_WriteAndReadRawValue_RoundTrip()
    {
        const string tableName = "Date32RoundTripTest";
        await RunTestWithTemporaryTable(TableSchema, tableName, async (connection, table) =>
        {
            const int originalRawValue = -50000;

            // Write using raw int value
            await new YdbCommand($"INSERT INTO `{table}` (Id, Date32Column) VALUES (@Id, CAST(@Date32Value AS Date32))",
                connection)
            {
                Parameters =
                {
                    new YdbParameter("@Id", 1),
                    new YdbParameter("@Date32Value", YdbDbType.Date32, originalRawValue)
                }
            }.ExecuteNonQueryAsync();

            // Read back using GetInt32
            var reader = await new YdbCommand($"SELECT Date32Column FROM `{table}` WHERE Id = 1", connection)
                .ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            var readValue = reader.GetInt32(0);
            Assert.Equal(originalRawValue, readValue);
        });
    }

    [Fact]
    public async Task Datetime64_WriteAndReadRawValue_RoundTrip()
    {
        const string tableName = "Datetime64RoundTripTest";
        await RunTestWithTemporaryTable(TableSchema, tableName, async (connection, table) =>
        {
            const long originalRawValue = -5000000000L;

            // Write using raw long value
            await new YdbCommand(
                $"INSERT INTO `{table}` (Id, Datetime64Column) VALUES (@Id, CAST(@Datetime64Value AS Datetime64))",
                connection)
            {
                Parameters =
                {
                    new YdbParameter("@Id", 1),
                    new YdbParameter("@Datetime64Value", YdbDbType.Datetime64, originalRawValue)
                }
            }.ExecuteNonQueryAsync();

            // Read back using GetInt64
            var reader = await new YdbCommand($"SELECT Datetime64Column FROM `{table}` WHERE Id = 1", connection)
                .ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            var readValue = reader.GetInt64(0);
            Assert.Equal(originalRawValue, readValue);
        });
    }

    [Fact]
    public async Task Timestamp64_WriteAndReadRawValue_RoundTrip()
    {
        const string tableName = "Timestamp64RoundTripTest";
        await RunTestWithTemporaryTable(TableSchema, tableName, async (connection, table) =>
        {
            const long originalRawValue = -5000000000000L;

            // Write using raw long value
            await new YdbCommand(
                $"INSERT INTO `{table}` (Id, Timestamp64Column) VALUES (@Id, CAST(@Timestamp64Value AS Timestamp64))",
                connection)
            {
                Parameters =
                {
                    new YdbParameter("@Id", 1),
                    new YdbParameter("@Timestamp64Value", YdbDbType.Timestamp64, originalRawValue)
                }
            }.ExecuteNonQueryAsync();

            // Read back using GetInt64
            var reader = await new YdbCommand($"SELECT Timestamp64Column FROM `{table}` WHERE Id = 1", connection)
                .ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            var readValue = reader.GetInt64(0);
            Assert.Equal(originalRawValue, readValue);
        });
    }

    [Fact]
    public async Task Interval64_WriteAndReadRawValue_RoundTrip()
    {
        const string tableName = "Interval64RoundTripTest";
        await RunTestWithTemporaryTable(TableSchema, tableName, async (connection, table) =>
        {
            const long originalRawValue = 5000000000000L;

            // Write using raw long value
            await new YdbCommand(
                $"INSERT INTO `{table}` (Id, Interval64Column) VALUES (@Id, CAST(@Interval64Value AS Interval64))",
                connection)
            {
                Parameters =
                {
                    new YdbParameter("@Id", 1),
                    new YdbParameter("@Interval64Value", YdbDbType.Interval64, originalRawValue)
                }
            }.ExecuteNonQueryAsync();

            // Read back using GetInt64
            var reader = await new YdbCommand($"SELECT Interval64Column FROM `{table}` WHERE Id = 1", connection)
                .ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            var readValue = reader.GetInt64(0);
            Assert.Equal(originalRawValue, readValue);
        });
    }

    [Fact]
    public async Task Date32_SupportsDateTime_WhenInRange()
    {
        const string tableName = "Date32DateTimeTest";
        await RunTestWithTemporaryTable(TableSchema, tableName, async (connection, table) =>
        {
            var testDate = new DateTime(2023, 10, 15);

            // Write using DateTime
            await new YdbCommand($"INSERT INTO `{table}` (Id, Date32Column) VALUES (@Id, @Date32Value)", connection)
            {
                Parameters =
                {
                    new YdbParameter("@Id", 1),
                    new YdbParameter("@Date32Value", YdbDbType.Date32, testDate)
                }
            }.ExecuteNonQueryAsync();

            // Read back using GetDateTime (should still work for values in range)
            var reader = await new YdbCommand($"SELECT Date32Column FROM `{table}` WHERE Id = 1", connection)
                .ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            var readDate = reader.GetDateTime(0);
            Assert.Equal(testDate.Date, readDate.Date);
        });
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public Task DisposeAsync() => Task.CompletedTask;
}
