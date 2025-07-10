using System.Data.Common;
using AdoNet.Specification.Tests;
using Xunit;

namespace Ydb.Sdk.Ado.Specification.Tests;

public class YdbDataReaderTests(YdbSelectValueFixture fixture) : DataReaderTestBase<YdbSelectValueFixture>(fixture)
{
    // UTF8 constant must have the suffix ''u!!! :(((
    public override void Dispose_command_before_reader()
    {
        using var connection = CreateOpenConnection();
        DbDataReader reader;
        using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT 'test'u;";
            reader = command.ExecuteReader();
        }

        Assert.True(reader.Read());
        Assert.Equal("test", reader.GetString(0));
        Assert.False(reader.Read());
    }

    public override void GetChars_reads_nothing_at_end_of_buffer() => TestGetChars(reader =>
    {
        Assert.Equal(0, reader.GetChars(0, 0, new char[4], 4, 0));
    });

    public override void GetChars_reads_nothing_when_dataOffset_is_too_large() => TestGetChars(reader =>
    {
        Assert.Equal(0, reader.GetChars(0, 6, new char[4], 0, 4));
    });

    public override void GetChars_reads_part_of_string() =>
        TestGetChars(reader =>
        {
            var buffer = new char[5];
            Assert.Equal(2, reader.GetChars(0, 1, buffer, 2, 2));
            Assert.Equal(new[] { '\0', '\0', 'b', '¢', '\0' }, buffer);
        });

    public override void GetChars_returns_length_when_buffer_is_null() => TestGetChars(reader =>
    {
        Assert.Equal(4, reader.GetChars(0, 0, null, 0, 0));
    });

    public override void GetChars_returns_length_when_buffer_is_null_and_dataOffset_is_specified() =>
        TestGetChars(reader => { Assert.Equal(4, reader.GetChars(0, 1, null, 0, 0)); });

    public override void GetChars_throws_when_bufferOffset_is_negative() =>
        TestGetChars(reader =>
        {
            AssertThrowsAny<ArgumentOutOfRangeException, IndexOutOfRangeException>(() =>
                reader.GetChars(0, 0, new char[4], -1, 4));
        });

    public override void GetChars_throws_when_bufferOffset_is_too_large() =>
        TestGetChars(reader =>
        {
            AssertThrowsAny<ArgumentOutOfRangeException, IndexOutOfRangeException>(() =>
                reader.GetChars(0, 0, new char[4], 5, 0));
        });

    public override void GetChars_throws_when_bufferOffset_plus_length_is_too_long() =>
        TestGetChars(reader =>
        {
            AssertThrowsAny<ArgumentException, IndexOutOfRangeException>(() =>
                reader.GetChars(0, 0, new char[4], 2, 3));
        });

    public override void GetChars_throws_when_dataOffset_is_negative() =>
        TestGetChars(reader =>
        {
            AssertThrowsAny<ArgumentOutOfRangeException, IndexOutOfRangeException, InvalidOperationException>(() =>
                reader.GetChars(0, -1, new char[4], 0, 4));
        });

    public override void GetChars_works()
    {
        using var connection = CreateOpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 'test'u;";
        using var reader = command.ExecuteReader();
        var hasData = reader.Read();
        Assert.True(hasData);

        var buffer = new char[4];
        Assert.Equal(4, reader.GetChars(0, 0, buffer, 0, buffer.Length));
        Assert.Equal(new[] { 't', 'e', 's', 't' }, buffer);
    }

    public override void GetChars_works_when_buffer_is_large() =>
        TestGetChars(reader =>
        {
            var buffer = new char[6];
            Assert.Equal(4, reader.GetChars(0, 0, buffer, 0, 6));
            Assert.Equal(new[] { 'a', 'b', '¢', 'd', '\0', '\0' }, buffer);
        });

    public override void GetFieldType_works()
    {
        using var connection = CreateOpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 'test'u;";
        using var reader = command.ExecuteReader();
        Assert.Equal(typeof(string), reader.GetFieldType(0));
    }

    public override void Item_by_ordinal_works()
    {
        using var connection = CreateOpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 'test'u;";
        using var reader = command.ExecuteReader();
        var hasData = reader.Read();
        Assert.True(hasData);

        Assert.Equal("test", reader[0]);
    }

    public override void Item_by_name_works()
    {
        using var connection = CreateOpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 'test'u AS Id;";
        using var reader = command.ExecuteReader();
        var hasData = reader.Read();
        Assert.True(hasData);

        Assert.Equal("test", reader["Id"]);
    }


    public override void GetValue_to_string_works_utf8_two_bytes() =>
        GetX_works("SELECT 'Ä'u;", r => r.GetValue(0) as string, "Ä");


    public override void GetValue_to_string_works_utf8_three_bytes() =>
        GetX_works("SELECT 'Ḁ'u;", r => r.GetValue(0) as string, "Ḁ");


    public override void GetValue_to_string_works_utf8_four_bytes() =>
        GetX_works("SELECT '😀'u;", r => r.GetValue(0) as string, "😀");

    public override void GetValues_works()
    {
        using var connection = CreateOpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 'a'u, NULL;";
        using var reader = command.ExecuteReader();
        var hasData = reader.Read();
        Assert.True(hasData);

        // Array may be wider than row
        var values = new object[3];
        var result = reader.GetValues(values);

        Assert.Equal(2, result);
        Assert.Equal("a", values[0]);
        Assert.Same(DBNull.Value, values[1]);
    }

    public override void GetString_works() => GetX_works("SELECT 'test'u;", r => r.GetString(0), "test");

    public override void GetString_works_utf8_two_bytes() => GetX_works("SELECT 'Ä'u;", r => r.GetString(0), "Ä");

    public override void GetString_works_utf8_three_bytes() => GetX_works("SELECT 'Ḁ'u;", r => r.GetString(0), "Ḁ");

    public override void GetString_works_utf8_four_bytes() => GetX_works("SELECT '😀'u;", r => r.GetString(0), "😀");

    // UNION does not guarantee order
    public override void Read_works()
    {
        using var connection = CreateOpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 as id UNION SELECT 2 as id ORDER BY id;";
        using var reader = command.ExecuteReader();
        var hasData = reader.Read();
        Assert.True(hasData);
        Assert.Equal(1L, reader.GetInt64(0));

        hasData = reader.Read();
        Assert.True(hasData);
        Assert.Equal(2L, reader.GetInt64(0));

        hasData = reader.Read();
        Assert.False(hasData);
    }

    public override void GetFieldValue_works_utf8_two_bytes() =>
        GetX_works("SELECT 'Ä'u;", r => r.GetFieldValue<string>(0), "Ä");

    public override void GetFieldValue_works_utf8_three_bytes() =>
        GetX_works("SELECT 'Ḁ'u;", r => r.GetFieldValue<string>(0), "Ḁ");

    public override void GetFieldValue_works_utf8_four_bytes() =>
        GetX_works("SELECT '😀'u;", r => r.GetFieldValue<string>(0), "😀");

#pragma warning disable xUnit1004
    [Fact(Skip = "Don't supported CommandBehavior")]
#pragma warning restore xUnit1004
    public override void SingleResult_returns_one_result_set()
    {
        base.SingleResult_returns_one_result_set();
    }

#pragma warning disable xUnit1004
    [Fact(Skip = "Don't supported CommandBehavior")]
#pragma warning restore xUnit1004
    public override void SingleRow_returns_one_result_set()
    {
        base.SingleRow_returns_one_result_set();
    }

#pragma warning disable xUnit1004
    [Fact(Skip = "Don't supported CommandBehavior")]
#pragma warning restore xUnit1004
    public override void SingleRow_returns_one_row()
    {
        base.SingleRow_returns_one_row();
    }


#pragma warning disable xUnit1004
    [Fact(Skip = "Mutually exclusive test with GetTextReader_throws_for_null_String")]
#pragma warning restore xUnit1004
    public override void GetTextReader_returns_empty_for_null_String()
    {
        base.GetTextReader_returns_empty_for_null_String();
    }

#pragma warning disable xUnit1004
    [Fact(Skip = "Not supported GetSchemaTable")]
#pragma warning restore xUnit1004
    public override void GetColumnSchema_is_empty_after_Delete()
    {
        base.GetColumnSchema_is_empty_after_Delete();
    }

#pragma warning disable xUnit1004
    [Fact(Skip = "Not supported GetSchemaTable")]
#pragma warning restore xUnit1004
    public override void GetColumnSchema_ColumnName()
    {
        base.GetColumnSchema_ColumnName();
    }

#pragma warning disable xUnit1004
    [Fact(Skip = "Not supported GetSchemaTable")]
#pragma warning restore xUnit1004
    public override void GetColumnSchema_DataType()
    {
        base.GetColumnSchema_DataType();
    }

#pragma warning disable xUnit1004
    [Fact(Skip = "Not supported GetSchemaTable")]
#pragma warning restore xUnit1004
    public override void GetColumnSchema_DataTypeName()
    {
        base.GetColumnSchema_DataTypeName();
    }

#pragma warning disable xUnit1004
    [Fact(Skip = "Not supported GetSchemaTable")]
#pragma warning restore xUnit1004
    public override void GetSchemaTable_is_null_after_Delete()
    {
        base.GetSchemaTable_is_null_after_Delete();
    }

    protected override async Task OnInitializeAsync()
    {
        await using var connection = CreateConnection();
        connection.ConnectionString = ConnectionString;
        await connection.OpenAsync();

        var ydbCommand = new YdbCommand
        {
            Connection = connection,
            CommandText =
                """
                CREATE TABLE `select_value`
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
                """
        };

        await ydbCommand.ExecuteNonQueryAsync();
        ydbCommand.CommandText =
            """
            INSERT INTO `select_value`(`Id`, `Binary`, `Boolean`, `Byte`, `SByte`, `Int16`, `UInt16`,`Int32`, 
            `UInt32`, `Int64`, `UInt64`, `Single`, `Double`, `Decimal`, `String`, `Guid`, `Date`, `DateTime`, `DateTime2`) VALUES
            (0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
            (1, '', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '', NULL, NULL, NULL, NULL),
            (2, String::HexDecode('00'), FALSE, 0, 0, 0, 0, 0, 0, 0, 0, CAST(0 AS Float), 0, CAST(0 AS Decimal(22, 9)), '0', Uuid('00000000-0000-0000-0000-000000000000'), NULL, NULL, CurrentUtcTimestamp()),
            (3, String::HexDecode('11'), TRUE, 1, 1, 1, 1, 1, 1, 1, 1, CAST(1 AS Float), 1, CAST(1 AS Decimal(22, 9)), '1', Uuid('11111111-1111-1111-1111-111111111111'), Date('2105-01-01'), Datetime('2105-01-01T11:11:11Z'), Timestamp('2105-01-01T11:11:11.111Z')),
            (4, NULL, FALSE, 0, -128, -32768, 0, -2147483648, 0, -9223372036854775808, 0, CAST(1.18e-38 AS Float), 2.23e-308, CAST('0.000000000000001' AS Decimal(22, 9)), NULL, Uuid('33221100-5544-7766-9988-aabbccddeeff'), Date('2000-01-01'), Datetime('2000-01-01T00:00:00Z'), Timestamp('2000-01-01T00:00:00.000Z')),
            (5, NULL, TRUE, 255, 127, 32767, 65535, 2147483647, 4294967295, 9223372036854775807, 18446744073709551615, CAST(3.40e38 AS Float), 1.79e308, CAST('99999999999999999999.999999999' AS Decimal(22, 9)), NULL, Uuid('ccddeeff-aabb-8899-7766-554433221100'), Date('1999-12-31'), Datetime('1999-12-31T23:59:59Z'), Timestamp('1999-12-31T23:59:59.999Z'));
            """;
        await ydbCommand.ExecuteNonQueryAsync();
    }

    protected override async Task OnDisposeAsync()
    {
        await using var connection = CreateConnection();
        connection.ConnectionString = ConnectionString;
        await connection.OpenAsync();

        await new YdbCommand { Connection = connection, CommandText = "DROP TABLE `select_value`" }
            .ExecuteNonQueryAsync();
    }

    // Copy and paste private method from base class
    private void TestGetChars(Action<DbDataReader> action)
    {
        using var connection = CreateOpenConnection();
        using var command = connection.CreateCommand();
        // NB: Intentionally using a multi-byte UTF-8 character
        command.CommandText = "SELECT 'ab¢d'u;";
        using var reader = command.ExecuteReader();
        reader.Read();
        action(reader);
    }

    private void GetX_works<T>(string sql, Func<DbDataReader, T> action, T expected)
    {
        using var connection = CreateOpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        using var reader = command.ExecuteReader();
        var hasData = reader.Read();

        Assert.True(hasData);
        Assert.Equal(expected, action(reader));
    }
}
