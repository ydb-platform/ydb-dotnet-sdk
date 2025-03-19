using System.Data;
using AdoNet.Specification.Tests;
using Xunit;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tests.Ado.Specification;

public class YdbGetValueConversionTests : GetValueConversionTestBase<YdbSelectValueFixture>
{
    public YdbGetValueConversionTests(YdbSelectValueFixture fixture) : base(fixture)
    {
    }

#pragma warning disable xUnit1004

    [Fact(Skip = "Uint16 >> Int16")]
    public override void GetInt16_for_minimum_UInt16() => base.GetInt16_for_minimum_UInt16();

    [Fact(Skip = "Uint16 >> Int16")]
    public override void GetInt16_for_minimum_UInt16_with_GetFieldValue() =>
        base.GetInt16_for_minimum_UInt16_with_GetFieldValue();

    [Fact(Skip = "Uint16 >> Int16")]
    public override Task GetInt16_for_minimum_UInt16_with_GetFieldValueAsync() =>
        base.GetInt16_for_minimum_UInt16_with_GetFieldValueAsync();

    [Fact(Skip = "Int32 >> Int16")]
    public override void GetInt16_for_one_Int32() => base.GetInt16_for_one_Int32();

    [Fact(Skip = "Int32 >> Int16")]
    public override void GetInt16_for_one_Int32_with_GetFieldValue() =>
        base.GetInt16_for_one_Int32_with_GetFieldValue();

    [Fact(Skip = "Int32 >> Int16")]
    public override Task GetInt16_for_one_Int32_with_GetFieldValueAsync() =>
        base.GetInt16_for_one_Int32_with_GetFieldValueAsync();

    [Fact(Skip = "Int64 >> Int16")]
    public override void GetInt16_for_one_Int64() => base.GetInt16_for_one_Int64();

    [Fact(Skip = "Int64 >> Int16")]
    public override void GetInt16_for_one_Int64_with_GetFieldValue() =>
        base.GetInt16_for_one_Int64_with_GetFieldValue();

    [Fact(Skip = "Int64 >> Int16")]
    public override Task GetInt16_for_one_Int64_with_GetFieldValueAsync() =>
        base.GetInt16_for_one_Int64_with_GetFieldValueAsync();

    [Fact(Skip = "Uint16 >> Int16")]
    public override void GetInt16_for_one_UInt16() => base.GetInt16_for_one_UInt16();

    [Fact(Skip = "Uint16 >> Int16")]
    public override void GetInt16_for_one_UInt16_with_GetFieldValue() =>
        base.GetInt16_for_one_UInt16_with_GetFieldValue();

    [Fact(Skip = "Uint16 >> Int16")]
    public override Task GetInt16_for_one_UInt16_with_GetFieldValueAsync() =>
        base.GetInt16_for_one_UInt16_with_GetFieldValueAsync();

    [Fact(Skip = "Int32 >> Int16")]
    public override void GetInt16_for_zero_Int32() => base.GetInt16_for_zero_Int32();

    [Fact(Skip = "Int32 >> Int16")]
    public override void GetInt16_for_zero_Int32_with_GetFieldValue() =>
        base.GetInt16_for_zero_Int32_with_GetFieldValue();

    [Fact(Skip = "Int32 >> Int16")]
    public override Task GetInt16_for_zero_Int32_with_GetFieldValueAsync() =>
        base.GetInt16_for_zero_Int32_with_GetFieldValueAsync();

    [Fact(Skip = "Int64 >> Int16")]
    public override void GetInt16_for_zero_Int64() => base.GetInt16_for_zero_Int64();

    [Fact(Skip = "Int64 >> Int16")]
    public override void GetInt16_for_zero_Int64_with_GetFieldValue() =>
        base.GetInt16_for_zero_Int64_with_GetFieldValue();

    [Fact(Skip = "Int64 >> Int16")]
    public override Task GetInt16_for_zero_Int64_with_GetFieldValueAsync() =>
        base.GetInt16_for_zero_Int64_with_GetFieldValueAsync();

    [Fact(Skip = "Uint16 >> Int16")]
    public override void GetInt16_for_zero_UInt16() => base.GetInt16_for_zero_UInt16();

    [Fact(Skip = "Uint16 >> Int16")]
    public override void GetInt16_for_zero_UInt16_with_GetFieldValue() =>
        base.GetInt16_for_zero_UInt16_with_GetFieldValue();

    [Fact(Skip = "Uint16 >> Int16")]
    public override Task GetInt16_for_zero_UInt16_with_GetFieldValueAsync() =>
        base.GetInt16_for_zero_UInt16_with_GetFieldValueAsync();

    [Fact(Skip = "Int32 >> Int16")]
    public override void GetInt16_throws_for_maximum_Int32() => base.GetInt16_throws_for_maximum_Int32();

    [Fact(Skip = "Int32 >> Int16")]
    public override void GetInt16_throws_for_maximum_Int32_with_GetFieldValue() =>
        base.GetInt16_throws_for_maximum_Int32_with_GetFieldValue();

    [Fact(Skip = "Int32 >> Int16")]
    public override Task GetInt16_throws_for_maximum_Int32_with_GetFieldValueAsync() =>
        base.GetInt16_throws_for_maximum_Int32_with_GetFieldValueAsync();

    [Fact(Skip = "Int64 >> Int16")]
    public override void GetInt16_throws_for_maximum_Int64() => base.GetInt16_throws_for_maximum_Int64();

    [Fact(Skip = "Int64 >> Int16")]
    public override void GetInt16_throws_for_maximum_Int64_with_GetFieldValue() =>
        base.GetInt16_throws_for_maximum_Int64_with_GetFieldValue();

    [Fact(Skip = "Int64 >> Int16")]
    public override Task GetInt16_throws_for_maximum_Int64_with_GetFieldValueAsync() =>
        base.GetInt16_throws_for_maximum_Int64_with_GetFieldValueAsync();

    [Fact(Skip = "Uint16 >> Int16")]
    public override void GetInt16_throws_for_maximum_UInt16() => base.GetInt16_throws_for_maximum_UInt16();

    [Fact(Skip = "Uint16 >> Int16")]
    public override void GetInt16_throws_for_maximum_UInt16_with_GetFieldValue() =>
        base.GetInt16_throws_for_maximum_UInt16_with_GetFieldValue();

    [Fact(Skip = "Uint16 >> Int16")]
    public override Task GetInt16_throws_for_maximum_UInt16_with_GetFieldValueAsync() =>
        base.GetInt16_throws_for_maximum_UInt16_with_GetFieldValueAsync();

    [Fact(Skip = "Int32 >> Int16")]
    public override void GetInt16_throws_for_minimum_Int32() => base.GetInt16_throws_for_minimum_Int32();

    [Fact(Skip = "Int32 >> Int16")]
    public override void GetInt16_throws_for_minimum_Int32_with_GetFieldValue() =>
        base.GetInt16_throws_for_minimum_Int32_with_GetFieldValue();

    [Fact(Skip = "Int32 >> Int16")]
    public override Task GetInt16_throws_for_minimum_Int32_with_GetFieldValueAsync() =>
        base.GetInt16_throws_for_minimum_Int32_with_GetFieldValueAsync();

    [Fact(Skip = "Int64 >> Int16")]
    public override void GetInt16_throws_for_minimum_Int64() => base.GetInt16_throws_for_minimum_Int64();

    [Fact(Skip = "Int64 >> Int16")]
    public override void GetInt16_throws_for_minimum_Int64_with_GetFieldValue() =>
        base.GetInt16_throws_for_minimum_Int64_with_GetFieldValue();

    [Fact(Skip = "Int64 >> Int16")]
    public override Task GetInt16_throws_for_minimum_Int64_with_GetFieldValueAsync() =>
        base.GetInt16_throws_for_minimum_Int64_with_GetFieldValueAsync();

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt32_for_maximum_Int16_with_GetFieldValue() =>
        base.GetInt32_for_maximum_Int16_with_GetFieldValue();

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt32_for_maximum_Int16_with_GetFieldValueAsync() =>
        base.GetInt32_for_maximum_Int16_with_GetFieldValueAsync();

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt32_for_maximum_UInt16_with_GetFieldValue() =>
        base.GetInt32_for_maximum_UInt16_with_GetFieldValue();

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt32_for_maximum_UInt16_with_GetFieldValueAsync() =>
        base.GetInt32_for_maximum_UInt16_with_GetFieldValueAsync();

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt32_for_minimum_Int16_with_GetFieldValue() =>
        base.GetInt32_for_minimum_Int16_with_GetFieldValue();

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt32_for_minimum_Int16_with_GetFieldValueAsync() =>
        base.GetInt32_for_minimum_Int16_with_GetFieldValueAsync();

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt32_for_minimum_UInt16_with_GetFieldValue() =>
        base.GetInt32_for_minimum_Int16_with_GetFieldValue();

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt32_for_minimum_UInt16_with_GetFieldValueAsync() =>
        base.GetInt32_for_minimum_UInt16_with_GetFieldValueAsync();

    [Fact(Skip = "Uint32 >> Int32")]
    public override void GetInt32_for_minimum_UInt32() => base.GetInt32_for_minimum_UInt32();

    [Fact(Skip = "Int64 >> Int16")]
    public override void GetInt32_for_minimum_UInt32_with_GetFieldValue() =>
        base.GetInt32_for_minimum_UInt32_with_GetFieldValue();

    [Fact(Skip = "Int64 >> Int16")]
    public override Task GetInt32_for_minimum_UInt32_with_GetFieldValueAsync() =>
        base.GetInt32_for_minimum_UInt32_with_GetFieldValueAsync();

    [Fact(Skip = "Int64 >> Int32")]
    public override void GetInt32_for_one_Int64() => base.GetInt16_for_one_Int64();

    [Fact(Skip = "Int64 >> Int32")]
    public override void GetInt32_for_one_Int64_with_GetFieldValue() =>
        base.GetInt32_for_one_Int64_with_GetFieldValue();

    [Fact(Skip = "Int64 >> Int32")]
    public override Task GetInt32_for_one_Int64_with_GetFieldValueAsync() =>
        base.GetInt32_for_one_Int64_with_GetFieldValueAsync();

    [Fact(Skip = "Uint32 >> Int32")]
    public override void GetInt32_for_one_UInt32() => base.GetInt32_for_one_UInt32();

    [Fact(Skip = "Uint32 >> Int32")]
    public override void GetInt32_for_one_UInt32_with_GetFieldValue() =>
        base.GetInt32_for_one_UInt32_with_GetFieldValue();

    [Fact(Skip = "Uint32 >> Int32")]
    public override Task GetInt32_for_one_UInt32_with_GetFieldValueAsync() =>
        base.GetInt32_for_one_UInt32_with_GetFieldValueAsync();

    [Fact(Skip = "UInt64 >> Int64")]
    public override void GetInt64_throws_for_maximum_UInt64() => base.GetInt64_throws_for_maximum_UInt64();

    [Fact(Skip = "UInt64 >> Int64")]
    public override void GetInt64_throws_for_maximum_UInt64_with_GetFieldValue() =>
        base.GetInt64_throws_for_maximum_UInt64_with_GetFieldValue();

    [Fact(Skip = "UInt64 >> Int64")]
    public override Task GetInt64_throws_for_maximum_UInt64_with_GetFieldValueAsync() =>
        base.GetInt64_throws_for_maximum_UInt64_with_GetFieldValueAsync();

    [Fact(Skip = "UInt64 >> Int64")]
    public override void GetInt64_for_zero_UInt64() => base.GetInt64_for_zero_UInt64();

    [Fact(Skip = "UInt64 >> Int64")]
    public override void GetInt64_for_zero_UInt64_with_GetFieldValue() =>
        base.GetInt64_for_zero_UInt64_with_GetFieldValue();

    [Fact(Skip = "UInt64 >> Int64")]
    public override Task GetInt64_for_zero_UInt64_with_GetFieldValueAsync() =>
        base.GetInt64_for_zero_UInt64_with_GetFieldValueAsync();

    [Fact(Skip = "UInt64 >> Int64")]
    public override void GetInt64_for_one_UInt64() => base.GetInt64_for_one_UInt64();

    [Fact(Skip = "UInt64 >> Int64")]
    public override void GetInt64_for_one_UInt64_with_GetFieldValue() =>
        base.GetInt64_for_one_UInt64_with_GetFieldValue();

    [Fact(Skip = "UInt64 >> Int64")]
    public override Task GetInt64_for_one_UInt64_with_GetFieldValueAsync() =>
        base.GetInt64_for_one_UInt64_with_GetFieldValueAsync();

    [Fact(Skip = "UInt64 >> Int64")]
    public override void GetInt64_for_minimum_UInt64() => base.GetInt64_for_minimum_UInt64();

    [Fact(Skip = "UInt64 >> Int64")]
    public override void GetInt64_for_minimum_UInt64_with_GetFieldValue() =>
        base.GetInt64_for_minimum_UInt64_with_GetFieldValue();

    [Fact(Skip = "UInt64 >> Int64")]
    public override Task GetInt64_for_minimum_UInt64_with_GetFieldValueAsync() =>
        base.GetInt64_for_minimum_UInt64_with_GetFieldValueAsync();

    public override void GetInt32_throws_for_maximum_Int64() => TestException(DbType.Int64, ValueKind.Maximum,
        x => x.GetInt32(0), typeof(InvalidCastException));

    public override void GetInt32_for_zero_Int64() => TestException(DbType.Int64, ValueKind.Zero, x => x.GetInt32(0),
        typeof(InvalidCastException));

    [Fact(Skip = "Uint32 >> Int32")]
    public override void GetInt32_for_zero_UInt32() => base.GetInt32_for_zero_UInt32();

    [Fact(Skip = "Uint32 >> Int32")]
    public override void GetInt32_for_zero_UInt32_with_GetFieldValue() =>
        base.GetInt32_for_zero_UInt32_with_GetFieldValue();

    [Fact(Skip = "Uint32 >> Int32")]
    public override Task GetInt32_for_zero_UInt32_with_GetFieldValueAsync() =>
        base.GetInt32_for_zero_UInt32_with_GetFieldValueAsync();

    public override void GetInt32_throws_for_maximum_UInt32() => TestException(DbType.UInt32, ValueKind.Maximum,
        x => x.GetInt32(0), typeof(InvalidCastException));

    public override void GetInt32_throws_for_maximum_UInt32_with_GetFieldValue() => TestException(DbType.UInt32,
        ValueKind.Maximum, x => x.GetFieldValue<int>(0), typeof(InvalidCastException));

    public override async Task GetInt32_throws_for_maximum_UInt32_with_GetFieldValueAsync() =>
        await TestExceptionAsync(DbType.UInt32, ValueKind.Maximum, async x => await x.GetFieldValueAsync<int>(0),
            typeof(InvalidCastException));

    public override Task GetInt32_throws_for_maximum_Int64_with_GetFieldValueAsync() =>
        TestExceptionAsync(DbType.Int64, ValueKind.Maximum, async x => await x.GetFieldValueAsync<int>(0),
            typeof(InvalidCastException));

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt32_for_one_Int16_with_GetFieldValue() =>
        TestGetValue(DbType.Int16, ValueKind.One, x => x.GetFieldValue<int>(0), 1);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt32_for_one_Int16_with_GetFieldValueAsync() => TestGetValueAsync(DbType.Int16,
        ValueKind.One, async x => await x.GetFieldValueAsync<int>(0), 1);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt32_for_one_UInt16_with_GetFieldValue() =>
        TestGetValue(DbType.UInt16, ValueKind.One, x => x.GetFieldValue<int>(0), 1);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt32_for_one_UInt16_with_GetFieldValueAsync() => TestGetValueAsync(DbType.UInt16,
        ValueKind.One, async x => await x.GetFieldValueAsync<int>(0), 1);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt32_for_zero_Int16_with_GetFieldValue() =>
        base.GetInt32_for_zero_Int16_with_GetFieldValue();

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt32_for_zero_Int16_with_GetFieldValueAsync() =>
        base.GetInt32_for_zero_Int16_with_GetFieldValueAsync();

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt32_for_zero_Int64_with_GetFieldValue() =>
        base.GetInt32_for_zero_Int64_with_GetFieldValue();

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt32_for_zero_Int64_with_GetFieldValueAsync() =>
        base.GetInt32_for_zero_Int64_with_GetFieldValueAsync();

    public override void GetInt32_throws_for_minimum_Int64() => TestException(DbType.Int64, ValueKind.Minimum,
        x => x.GetInt32(0), typeof(InvalidCastException));

    public override void GetInt32_throws_for_minimum_Int64_with_GetFieldValue() => TestException(DbType.Int64,
        ValueKind.Minimum, x => x.GetFieldValue<int>(0), typeof(InvalidCastException));

    public override Task GetInt32_throws_for_minimum_Int64_with_GetFieldValueAsync() =>
        TestExceptionAsync(DbType.Int64, ValueKind.Minimum, async x => await x.GetFieldValueAsync<int>(0),
            typeof(InvalidCastException));

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt32_for_zero_UInt16_with_GetFieldValue() =>
        TestGetValue(DbType.UInt16, ValueKind.Zero, x => x.GetFieldValue<int>(0), 0);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt32_for_zero_UInt16_with_GetFieldValueAsync() => TestGetValueAsync(DbType.UInt16,
        ValueKind.Zero, async x => await x.GetFieldValueAsync<int>(0), 0);

    public override void GetInt32_throws_for_maximum_Int64_with_GetFieldValue() => TestException(DbType.Int64,
        ValueKind.Maximum, x => x.GetFieldValue<int>(0), typeof(InvalidCastException));

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt64_for_maximum_Int16_with_GetFieldValue() =>
        base.GetInt64_for_maximum_Int16_with_GetFieldValue();

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt64_for_maximum_Int16_with_GetFieldValueAsync() =>
        base.GetInt64_for_maximum_Int16_with_GetFieldValueAsync();

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt64_for_maximum_Int32_with_GetFieldValue() => TestGetValue(DbType.Int32,
        ValueKind.Maximum, x => x.GetFieldValue<long>(0), 2147483647L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt64_for_maximum_Int32_with_GetFieldValueAsync() =>
        TestGetValueAsync(DbType.Int32, ValueKind.Maximum, async x => await x.GetFieldValueAsync<long>(0),
            2147483647L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt64_for_maximum_UInt16_with_GetFieldValue() =>
        TestGetValue(DbType.UInt16, ValueKind.Maximum, x => x.GetFieldValue<long>(0), 65535L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt64_for_maximum_UInt16_with_GetFieldValueAsync() =>
        TestGetValueAsync(DbType.UInt16, ValueKind.Maximum, async x => await x.GetFieldValueAsync<long>(0),
            65535L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt64_for_minimum_Int16_with_GetFieldValue() =>
        TestGetValue(DbType.Int16, ValueKind.Minimum, x => x.GetFieldValue<long>(0), -32768L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt64_for_minimum_Int16_with_GetFieldValueAsync() =>
        TestGetValueAsync(DbType.Int16, ValueKind.Minimum, async x => await x.GetFieldValueAsync<long>(0),
            -32768L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt64_for_minimum_Int32_with_GetFieldValue() => TestGetValue(DbType.Int32,
        ValueKind.Minimum, x => x.GetFieldValue<long>(0), -2147483648L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt64_for_minimum_Int32_with_GetFieldValueAsync() =>
        TestGetValueAsync(DbType.Int32, ValueKind.Minimum, async x => await x.GetFieldValueAsync<long>(0),
            -2147483648L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt64_for_minimum_UInt16_with_GetFieldValue() =>
        TestGetValue(DbType.UInt16, ValueKind.Minimum, x => x.GetFieldValue<long>(0), 0L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt64_for_minimum_UInt16_with_GetFieldValueAsync() => TestGetValueAsync(DbType.UInt16,
        ValueKind.Minimum, async x => await x.GetFieldValueAsync<long>(0), 0L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt64_for_one_Int16_with_GetFieldValue() =>
        TestGetValue(DbType.Int16, ValueKind.One, x => x.GetFieldValue<long>(0), 1L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt64_for_one_Int16_with_GetFieldValueAsync() => TestGetValueAsync(DbType.Int16,
        ValueKind.One, async x => await x.GetFieldValueAsync<long>(0), 1L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt64_for_one_Int32_with_GetFieldValue() =>
        TestGetValue(DbType.Int32, ValueKind.One, x => x.GetFieldValue<long>(0), 1L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt64_for_one_Int32_with_GetFieldValueAsync() => TestGetValueAsync(DbType.Int32,
        ValueKind.One, async x => await x.GetFieldValueAsync<long>(0), 1L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt64_for_one_UInt16_with_GetFieldValue() =>
        TestGetValue(DbType.UInt16, ValueKind.One, x => x.GetFieldValue<long>(0), 1L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt64_for_one_UInt16_with_GetFieldValueAsync() => TestGetValueAsync(DbType.UInt16,
        ValueKind.One, async x => await x.GetFieldValueAsync<long>(0), 1L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt64_for_zero_Int16_with_GetFieldValue() =>
        TestGetValue(DbType.Int16, ValueKind.Zero, x => x.GetFieldValue<long>(0), 0L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt64_for_zero_Int16_with_GetFieldValueAsync() => TestGetValueAsync(DbType.Int16,
        ValueKind.Zero, async x => await x.GetFieldValueAsync<long>(0), 0L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt64_for_zero_Int32_with_GetFieldValue() =>
        TestGetValue(DbType.Int32, ValueKind.Zero, x => x.GetFieldValue<long>(0), 0L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt64_for_zero_Int32_with_GetFieldValueAsync() => TestGetValueAsync(DbType.Int32,
        ValueKind.Zero, async x => await x.GetFieldValueAsync<long>(0), 0L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetInt64_for_zero_UInt16_with_GetFieldValue() =>
        TestGetValue(DbType.UInt16, ValueKind.Zero, x => x.GetFieldValue<long>(0), 0L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetInt64_for_zero_UInt16_with_GetFieldValueAsync() => TestGetValueAsync(DbType.UInt16,
        ValueKind.Zero, async x => await x.GetFieldValueAsync<long>(0), 0L);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetDouble_for_zero_Single_with_GetFieldValueAsync() =>
        base.GetDouble_for_zero_Single_with_GetFieldValueAsync();

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetDouble_for_zero_Single_with_GetFieldValue() =>
        base.GetDouble_for_zero_Single_with_GetFieldValue();

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetDouble_for_maximum_Single_with_GetFieldValue() => TestGetValue(DbType.Single,
        ValueKind.Maximum, x => x.GetFieldValue<double>(0), 3.3999999521443642E+38);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetDouble_for_maximum_Single_with_GetFieldValueAsync() =>
        TestGetValueAsync(DbType.Single, ValueKind.Maximum, async x => await x.GetFieldValueAsync<double>(0),
            3.3999999521443642E+38);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetDouble_for_one_Single_with_GetFieldValue() =>
        TestGetValue(DbType.Single, ValueKind.One, x => x.GetFieldValue<double>(0), 1.0);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetDouble_for_one_Single_with_GetFieldValueAsync() => TestGetValueAsync(DbType.Single,
        ValueKind.One, async x => await x.GetFieldValueAsync<double>(0), 1.0);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override void GetDouble_for_minimum_Single_with_GetFieldValue() => TestGetValue(DbType.Single,
        ValueKind.Minimum, x => x.GetFieldValue<double>(0), 1.1799999457746311E-38);

    [Fact(Skip = "Method GetFieldValue is not fully supported")]
    public override Task GetDouble_for_minimum_Single_with_GetFieldValueAsync() =>
        TestGetValueAsync(DbType.Single, ValueKind.Minimum, async x => await x.GetFieldValueAsync<double>(0),
            1.1799999457746311E-38);

#pragma warning restore xUnit1004
    protected override async Task OnInitializeAsync()
    {
        await using var connection = CreateConnection();
        connection.ConnectionString = ConnectionString;
        await connection.OpenAsync();

        var ydbCommand = new YdbCommand
        {
            Connection = connection,
            CommandText = $@"
            CREATE TABLE `select_value_{Utils.Net}`
		 	(
		 		`Id` Int32 NOT NULL,
		 		`Binary` Bytes,
		 		`Boolean` Bool,
		 		`Byte` Uint8,
		 		`SByte` Int8,
		 		`Int16` Int16,
		 		`UInt16` UInt16,
		 		`Int32` Int32,
		 		`UInt32` UInt32,
		 		`Int64` Int64,
		 		`UInt64` UInt64,
		 		`Single` Float,
		 		`Double` Double,
		 		`Decimal` Decimal(22, 9),
		 		`String` Text,
		 		`Guid` Uuid,
		 		`Date` Date,
		 		`DateTime` Datetime,
				`DateTime2` Timestamp,
		 		
		 		PRIMARY KEY (`Id`)
		 	);
            "
        };

        await ydbCommand.ExecuteNonQueryAsync();
        ydbCommand.CommandText = $@"
			INSERT INTO `select_value_{Utils.Net}`(`Id`, `Binary`, `Boolean`, `Byte`, `SByte`, `Int16`, `UInt16`,`Int32`, 
			`UInt32`, `Int64`, `UInt64`, `Single`, `Double`, `Decimal`, `String`, `Guid`, `Date`, `DateTime`, `DateTime2`) VALUES
		 	(0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
		 	(1, '', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '', NULL, NULL, NULL, NULL),
		 	(2, String::HexDecode('00'), FALSE, 0, 0, 0, 0, 0, 0, 0, 0, CAST(0 AS Float), 0, CAST(0 AS Decimal(22, 9)), '0', Uuid('00000000-0000-0000-0000-000000000000'), NULL, NULL, CurrentUtcTimestamp()),
		 	(3, String::HexDecode('11'), TRUE, 1, 1, 1, 1, 1, 1, 1, 1, CAST(1 AS Float), 1, CAST(1 AS Decimal(22, 9)), '1', Uuid('11111111-1111-1111-1111-111111111111'), Date('2105-01-01'), Datetime('2105-01-01T11:11:11Z'), Timestamp('2105-01-01T11:11:11.111Z')),
		 	(4, NULL, FALSE, 0, -128, -32768, 0, -2147483648, 0, -9223372036854775808, 0, CAST(1.18e-38 AS Float), 2.23e-308, CAST('0.000000000000001' AS Decimal(22, 9)), NULL, Uuid('33221100-5544-7766-9988-aabbccddeeff'), Date('2000-01-01'), Datetime('2000-01-01T00:00:00Z'), Timestamp('2000-01-01T00:00:00.000Z')),
		 	(5, NULL, TRUE, 255, 127, 32767, 65535, 2147483647, 4294967295, 9223372036854775807, 18446744073709551615, CAST(3.40e38 AS Float), 1.79e308, CAST('99999999999999999999.999999999' AS Decimal(22, 9)), NULL, Uuid('ccddeeff-aabb-8899-7766-554433221100'), Date('1999-12-31'), Datetime('1999-12-31T23:59:59Z'), Timestamp('1999-12-31T23:59:59.999Z'));
			";
        await ydbCommand.ExecuteNonQueryAsync();
    }

    protected override async Task OnDisposeAsync()
    {
        await using var connection = CreateConnection();
        connection.ConnectionString = ConnectionString;
        await connection.OpenAsync();

        await new YdbCommand { Connection = connection, CommandText = $"DROP TABLE `select_value_{Utils.Net}`" }
            .ExecuteNonQueryAsync();
    }
}
