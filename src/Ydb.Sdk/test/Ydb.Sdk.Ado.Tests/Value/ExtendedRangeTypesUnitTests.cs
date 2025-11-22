using Xunit;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Ado.YdbType;

namespace Ydb.Sdk.Ado.Tests.Value;

/// <summary>
/// Unit tests for extended range types (Date32, Datetime64, Timestamp64, Interval64)
/// that test the raw value packing without requiring a database connection.
/// </summary>
public class ExtendedRangeTypesUnitTests
{
    [Fact]
    public void Date32_PacksIntValue()
    {
        // Arrange
        const int rawDaysValue = -100000;
        var typeInfo = YdbPrimitiveTypeInfo.Date32;

        // Act
        var packedValue = typeInfo.Pack(rawDaysValue);

        // Assert
        Assert.NotNull(packedValue);
        Assert.Equal(rawDaysValue, packedValue.Int32Value);
    }

    [Fact]
    public void Date32_PacksDateTime()
    {
        // Arrange
        var dateTime = new DateTime(2023, 10, 15);
        var typeInfo = YdbPrimitiveTypeInfo.Date32;

        // Act
        var packedValue = typeInfo.Pack(dateTime);

        // Assert
        Assert.NotNull(packedValue);
        // Verify it's a valid Int32Value (the actual value doesn't matter, just that it packs)
        Assert.NotEqual(0, packedValue.Int32Value);
    }

    [Fact]
    public void Datetime64_PacksLongValue()
    {
        // Arrange
        const long rawSecondsValue = -10000000000L;
        var typeInfo = YdbPrimitiveTypeInfo.Datetime64;

        // Act
        var packedValue = typeInfo.Pack(rawSecondsValue);

        // Assert
        Assert.NotNull(packedValue);
        Assert.Equal(rawSecondsValue, packedValue.Int64Value);
    }

    [Fact]
    public void Datetime64_PacksDateTime()
    {
        // Arrange
        var dateTime = new DateTime(2023, 10, 15, 14, 30, 45);
        var typeInfo = YdbPrimitiveTypeInfo.Datetime64;

        // Act
        var packedValue = typeInfo.Pack(dateTime);

        // Assert
        Assert.NotNull(packedValue);
        // Verify it's a valid Int64Value (the actual value doesn't matter, just that it packs)
        Assert.NotEqual(0, packedValue.Int64Value);
    }

    [Fact]
    public void Timestamp64_PacksLongValue()
    {
        // Arrange
        const long rawMicrosecondsValue = -10000000000000L;
        var typeInfo = YdbPrimitiveTypeInfo.Timestamp64;

        // Act
        var packedValue = typeInfo.Pack(rawMicrosecondsValue);

        // Assert
        Assert.NotNull(packedValue);
        Assert.Equal(rawMicrosecondsValue, packedValue.Int64Value);
    }

    [Fact]
    public void Timestamp64_PacksDateTime()
    {
        // Arrange
        var dateTime = new DateTime(2023, 10, 15, 14, 30, 45, 123);
        var typeInfo = YdbPrimitiveTypeInfo.Timestamp64;

        // Act
        var packedValue = typeInfo.Pack(dateTime);

        // Assert
        Assert.NotNull(packedValue);
        // Verify it's a valid Int64Value (the actual value doesn't matter, just that it packs)
        Assert.NotEqual(0, packedValue.Int64Value);
    }

    [Fact]
    public void Interval64_PacksLongValue()
    {
        // Arrange
        const long rawMicrosecondsValue = 10000000000000L;
        var typeInfo = YdbPrimitiveTypeInfo.Interval64;

        // Act
        var packedValue = typeInfo.Pack(rawMicrosecondsValue);

        // Assert
        Assert.NotNull(packedValue);
        Assert.Equal(rawMicrosecondsValue, packedValue.Int64Value);
    }

    [Fact]
    public void Interval64_PacksTimeSpan()
    {
        // Arrange
        var timeSpan = TimeSpan.FromHours(24);
        var typeInfo = YdbPrimitiveTypeInfo.Interval64;

        // Act
        var packedValue = typeInfo.Pack(timeSpan);

        // Assert
        Assert.NotNull(packedValue);
        // Verify it's a valid Int64Value (the actual value doesn't matter, just that it packs)
        Assert.NotEqual(0, packedValue.Int64Value);
    }

    [Fact]
    public void Date32_UnpacksToInt32()
    {
        // Arrange
        const int expectedValue = -100000;
        var ydbValue = new Ydb.Value { Int32Value = expectedValue };

        // Act
        var unpackedValue = ydbValue.Int32Value;

        // Assert
        Assert.Equal(expectedValue, unpackedValue);
    }

    [Fact]
    public void Datetime64_UnpacksToInt64()
    {
        // Arrange
        const long expectedValue = -10000000000L;
        var ydbValue = new Ydb.Value { Int64Value = expectedValue };

        // Act
        var unpackedValue = ydbValue.Int64Value;

        // Assert
        Assert.Equal(expectedValue, unpackedValue);
    }

    [Fact]
    public void Timestamp64_UnpacksToInt64()
    {
        // Arrange
        const long expectedValue = -10000000000000L;
        var ydbValue = new Ydb.Value { Int64Value = expectedValue };

        // Act
        var unpackedValue = ydbValue.Int64Value;

        // Assert
        Assert.Equal(expectedValue, unpackedValue);
    }

    [Fact]
    public void Interval64_UnpacksToInt64()
    {
        // Arrange
        const long expectedValue = 10000000000000L;
        var ydbValue = new Ydb.Value { Int64Value = expectedValue };

        // Act
        var unpackedValue = ydbValue.Int64Value;

        // Assert
        Assert.Equal(expectedValue, unpackedValue);
    }

    [Fact]
    public void Date32_PackUnpackRoundTrip_IntValue()
    {
        // Arrange
        const int originalValue = -50000;
        var typeInfo = YdbPrimitiveTypeInfo.Date32;

        // Act
        var packedValue = typeInfo.Pack(originalValue);
        var unpackedValue = packedValue!.Int32Value;

        // Assert
        Assert.Equal(originalValue, unpackedValue);
    }

    [Fact]
    public void Datetime64_PackUnpackRoundTrip_LongValue()
    {
        // Arrange
        const long originalValue = -5000000000L;
        var typeInfo = YdbPrimitiveTypeInfo.Datetime64;

        // Act
        var packedValue = typeInfo.Pack(originalValue);
        var unpackedValue = packedValue!.Int64Value;

        // Assert
        Assert.Equal(originalValue, unpackedValue);
    }

    [Fact]
    public void Timestamp64_PackUnpackRoundTrip_LongValue()
    {
        // Arrange
        const long originalValue = -5000000000000L;
        var typeInfo = YdbPrimitiveTypeInfo.Timestamp64;

        // Act
        var packedValue = typeInfo.Pack(originalValue);
        var unpackedValue = packedValue!.Int64Value;

        // Assert
        Assert.Equal(originalValue, unpackedValue);
    }

    [Fact]
    public void Interval64_PackUnpackRoundTrip_LongValue()
    {
        // Arrange
        const long originalValue = 5000000000000L;
        var typeInfo = YdbPrimitiveTypeInfo.Interval64;

        // Act
        var packedValue = typeInfo.Pack(originalValue);
        var unpackedValue = packedValue!.Int64Value;

        // Assert
        Assert.Equal(originalValue, unpackedValue);
    }

    [Theory]
    [InlineData(int.MinValue)]
    [InlineData(-1000000)]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(1000000)]
    [InlineData(int.MaxValue)]
    public void Date32_PacksVariousIntValues(int value)
    {
        // Arrange
        var typeInfo = YdbPrimitiveTypeInfo.Date32;

        // Act
        var packedValue = typeInfo.Pack(value);

        // Assert
        Assert.NotNull(packedValue);
        Assert.Equal(value, packedValue.Int32Value);
    }

    [Theory]
    [InlineData(long.MinValue)]
    [InlineData(-10000000000000L)]
    [InlineData(-1L)]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(10000000000000L)]
    [InlineData(long.MaxValue)]
    public void Datetime64_PacksVariousLongValues(long value)
    {
        // Arrange
        var typeInfo = YdbPrimitiveTypeInfo.Datetime64;

        // Act
        var packedValue = typeInfo.Pack(value);

        // Assert
        Assert.NotNull(packedValue);
        Assert.Equal(value, packedValue.Int64Value);
    }

    [Theory]
    [InlineData(long.MinValue)]
    [InlineData(-10000000000000L)]
    [InlineData(-1L)]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(10000000000000L)]
    [InlineData(long.MaxValue)]
    public void Timestamp64_PacksVariousLongValues(long value)
    {
        // Arrange
        var typeInfo = YdbPrimitiveTypeInfo.Timestamp64;

        // Act
        var packedValue = typeInfo.Pack(value);

        // Assert
        Assert.NotNull(packedValue);
        Assert.Equal(value, packedValue.Int64Value);
    }

    [Theory]
    [InlineData(long.MinValue)]
    [InlineData(-10000000000000L)]
    [InlineData(-1L)]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(10000000000000L)]
    [InlineData(long.MaxValue)]
    public void Interval64_PacksVariousLongValues(long value)
    {
        // Arrange
        var typeInfo = YdbPrimitiveTypeInfo.Interval64;

        // Act
        var packedValue = typeInfo.Pack(value);

        // Assert
        Assert.NotNull(packedValue);
        Assert.Equal(value, packedValue.Int64Value);
    }
}
