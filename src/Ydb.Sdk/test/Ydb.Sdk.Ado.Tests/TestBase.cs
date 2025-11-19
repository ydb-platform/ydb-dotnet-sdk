using Xunit;
using Ydb.Sdk.Ado.Tests.Utils;

namespace Ydb.Sdk.Ado.Tests;

public abstract class TestBase : IAsyncLifetime
{
    protected static string ConnectionString => TestUtils.ConnectionString;

    protected static YdbConnection CreateConnection() => new(
        new YdbConnectionStringBuilder(ConnectionString) { LoggerFactory = TestUtils.LoggerFactory }
    );

    protected static YdbConnection CreateOpenConnection()
    {
        var connection = CreateConnection();
        connection.Open();
        return connection;
    }

    protected static async Task<YdbConnection> CreateOpenConnectionAsync()
    {
        var connection = CreateConnection();
        await connection.OpenAsync();
        return connection;
    }

    protected static async Task RunTestWithTemporaryTable(string sqlTableFormat, string tableName,
        Func<YdbConnection, string, Task> test)
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        await new YdbCommand(string.Format(sqlTableFormat, tableName), ydbConnection).ExecuteNonQueryAsync();
        await test(ydbConnection, tableName).ConfigureAwait(false);
        await new YdbCommand($"DROP TABLE `{tableName}`", ydbConnection).ExecuteNonQueryAsync();
    }

    protected static string AllTypesTable(bool nullable = false)
    {
        const string schema = """
                              CREATE TABLE `{0}` (
                                  Int32Column Int32 NOT NULL,
                                  BoolColumn Bool NOT NULL,
                                  Int64Column Int64 NOT NULL,
                                  Int16Column Int16 NOT NULL,
                                  Int8Column Int8 NOT NULL,
                                  FloatColumn Float NOT NULL,
                                  DoubleColumn Double NOT NULL,
                                  DefaultDecimalColumn Decimal(22, 9) NOT NULL,
                                  CustomDecimalColumn Decimal(35, 5) NOT NULL,
                                  Uint8Column Uint8 NOT NULL,
                                  Uint16Column Uint16 NOT NULL,
                                  Uint32Column Uint32 NOT NULL,
                                  Uint64Column Uint64 NOT NULL,
                                  TextColumn Text NOT NULL,
                                  BytesColumn Bytes NOT NULL,
                                  DateColumn Date NOT NULL,
                                  DatetimeColumn Datetime NOT NULL,
                                  TimestampColumn Timestamp NOT NULL,
                                  IntervalColumn Interval NOT NULL,
                                  JsonColumn Json NOT NULL,
                                  JsonDocumentColumn JsonDocument NOT NULL,
                                  YsonColumn Yson NOT NULL,
                                  Date32Column Date32 NOT NULL,
                                  Datetime64Column DateTime64 NOT NULL,
                                  Timestamp64Column Timestamp64 NOT NULL,
                                  Interval64Column Interval64 NOT NULL,
                                  PRIMARY KEY (Int32Column)
                              )
                              """;

        return nullable ? schema.Replace(" NOT NULL", string.Empty) : schema;
    }

    protected static string InsertAllTypesTable(string tableName) =>
        $"""
         INSERT INTO `{tableName}` (
             Int32Column, BoolColumn, Int64Column, Int16Column, Int8Column, FloatColumn, DoubleColumn, 
             DefaultDecimalColumn, CustomDecimalColumn, Uint8Column, Uint16Column, Uint32Column, 
             Uint64Column, TextColumn, BytesColumn, DateColumn, DatetimeColumn, TimestampColumn,
             IntervalColumn, JsonColumn, JsonDocumentColumn, YsonColumn, Date32Column, Datetime64Column,
             Timestamp64Column,  Interval64Column
         ) VALUES (
             @Int32Column, @BoolColumn, @Int64Column, @Int16Column, @Int8Column, @FloatColumn, 
             @DoubleColumn, @DefaultDecimalColumn, @CustomDecimalColumn, @Uint8Column, @Uint16Column, 
             @Uint32Column, @Uint64Column, @TextColumn, @BytesColumn, @DateColumn, @DatetimeColumn, 
             @TimestampColumn, @IntervalColumn, @JsonColumn, @JsonDocumentColumn, @YsonColumn, @Date32Column,
             @Datetime64Column,  @Timestamp64Column, @Interval64Column
         );
         """;

    protected static string SelectAllTypesTable(string tableName) =>
        $"""
         SELECT 
             Int32Column, BoolColumn, Int64Column, Int16Column, Int8Column, FloatColumn, DoubleColumn, 
             DefaultDecimalColumn, CustomDecimalColumn, Uint8Column, Uint16Column, Uint32Column, 
             Uint64Column, TextColumn, BytesColumn, DateColumn, DatetimeColumn, TimestampColumn,
             IntervalColumn, JsonColumn, JsonDocumentColumn, YsonColumn, Date32Column, Datetime64Column,  
             Timestamp64Column, Interval64Column
         FROM `{tableName}` 
         ORDER BY Int32Column;
         """;

    public async Task InitializeAsync() => await OnInitializeAsync().ConfigureAwait(false);

    public async Task DisposeAsync() => await OnDisposeAsync().ConfigureAwait(false);

    protected virtual Task OnInitializeAsync() => Task.CompletedTask;

    protected virtual Task OnDisposeAsync() => Task.CompletedTask;
}
