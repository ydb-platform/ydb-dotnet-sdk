using Google.Protobuf.Collections;
using Xunit;
using Ydb.Sdk.Ado.RetryPolicy;
using Ydb.Sdk.Ado.Schema;
using Ydb.Sdk.Ado.YdbType;

namespace Ydb.Sdk.Ado.Tests;

public class YdbDataSourceTests : TestBase
{
    private const int SelectedCount = 100;

    private readonly YdbDataSource _dataSource = new(ConnectionString);

    [Fact]
    public async Task OpenConnectionAsync_WhenMaxPoolSizeIs10_ReturnOpenConnection()
    {
        var tasks = new Task[SelectedCount];
        for (var i = 0; i < SelectedCount; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                await using var ydbConnection = await _dataSource.OpenConnectionAsync();
                var ydbCommand = new YdbCommand(ydbConnection) { CommandText = "SELECT 1" };
                Assert.Equal(1, await ydbCommand.ExecuteScalarAsync());
            });
        }

        await Task.WhenAll(tasks);
    }

    [Fact]
    public void CreateCommand_FromDataSource_ReturnDbCommand()
    {
        var dataSource = new YdbDataSource(ConnectionString + ";MaxPoolSize=5");
        for (var i = 0; i < SelectedCount; i++)
        {
            using var command = dataSource.CreateCommand("SELECT 1;");
            Assert.Equal(1, command.ExecuteScalar());
        }

        dataSource.Dispose();
        for (var i = 0; i < SelectedCount; i++)
        {
            using var command = dataSource.CreateCommand("SELECT 1;");
            Assert.Equal(1, command.ExecuteScalar());
        }

        dataSource.Dispose();
    }

    [Fact]
    public void CreateConnection_FromDataSource_ReturnNotOpenConnection()
    {
        using var ydbConnection = _dataSource.CreateConnection();
        ydbConnection.Open();
        Assert.Equal(1, new YdbCommand(ydbConnection) { CommandText = "SELECT 1" }.ExecuteScalar());
    }

    [Fact]
    public void OpenConnection_FromDataSource_ReturnOpenConnection()
    {
        using var ydbConnection = _dataSource.OpenConnection();
        Assert.Equal(1, new YdbCommand(ydbConnection) { CommandText = "SELECT 1" }.ExecuteScalar());
    }

    [Fact]
    public async Task ExecuteAsync_WhenBadSession_ThenRetriesUntilSuccess()
    {
        var attempt = 0;
        await _dataSource.ExecuteAsync(_ =>
        {
            if (attempt++ < 3)
            {
                throw new YdbException(StatusCode.BadSession, "Bad Session");
            }

            return Task.CompletedTask;
        });
    }

    [Theory]
    [InlineData(StatusCode.Undetermined)]
    [InlineData(StatusCode.ClientTransportUnknown)]
    [InlineData(StatusCode.ClientTransportUnavailable)]
    public async Task ExecuteAsync_WhenIsIdempotenceConfig_ThenRetriesUntilSuccess(StatusCode statusCode)
    {
        var attempt = 0;
        await _dataSource.ExecuteAsync(_ =>
        {
            if (attempt++ < 3)
            {
                throw new YdbException(statusCode, "Bad Session");
            }

            return Task.CompletedTask;
        }, new YdbRetryPolicyConfig { EnableRetryIdempotence = true });
    }

    [Theory]
    [InlineData(StatusCode.BadRequest)]
    [InlineData(StatusCode.SchemeError)]
    [InlineData(StatusCode.NotFound)]
    public async Task ExecuteAsync_WhenNonRetryableStatus_ThenThrowsWithoutRetry(StatusCode code)
    {
        var attempt = 0;

        var ex = await Assert.ThrowsAsync<YdbException>(() =>
            _dataSource.ExecuteAsync(_ =>
            {
                attempt++;
                throw new YdbException(code, "Non-retryable");
            }));

        Assert.Equal(code, ex.Code);
        Assert.Equal(1, attempt);
    }

    [Fact]
    public async Task ExecuteAsync_WhenAlwaysRetryableAndMaxAttemptsReached_ThenThrowsLastException()
    {
        var attempt = 0;
        var config = new YdbRetryPolicyConfig { MaxAttempts = 3 }; // как у вас конфигурируется

        var ex = await Assert.ThrowsAsync<YdbException>(() => _dataSource.ExecuteAsync(_ =>
        {
            attempt++;
            throw new YdbException(StatusCode.BadSession, "Still bad");
        }, config));

        Assert.Equal(3, attempt);
        Assert.Equal(StatusCode.BadSession, ex.Code);
    }

    [Fact]
    public async Task ExecuteAsync_WhenTokenPreCanceled_ThenDoesNotInvokeDelegate()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var called = false;
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            _dataSource.ExecuteAsync((_, _) =>
            {
                called = true;
                return Task.CompletedTask;
            }, cts.Token));

        Assert.False(called);
    }

    [Fact]
    public async Task ExecuteAsync_WhenUserCodeThrows_ThenDoesNotRetry()
    {
        var attempt = 0;
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _dataSource.ExecuteAsync(_ =>
            {
                attempt++;
                throw new InvalidOperationException("Bug");
            }));

        Assert.Equal(1, attempt);
    }

    [Fact]
    public async Task ExecuteAsync_WhenBadSession_ThenCreatesNewSessionPerAttempt()
    {
        var ydbConnections = new List<YdbConnection>();

        var attempt = 0;
        await _dataSource.ExecuteAsync(ydbConnection =>
        {
            ydbConnection.OnNotSuccessStatusCode(StatusCode.BadSession);
            ydbConnections.Add(ydbConnection);
            if (attempt++ < 2)
                throw new YdbException(StatusCode.BadSession, "Bad");
            return Task.CompletedTask;
        });

        Assert.Equal(3, attempt);
        Assert.Equal(3, ydbConnections.Count);
        Assert.True(ydbConnections.Distinct().Count() == ydbConnections.Count); // new one every time
    }

    [Fact]
    public async Task ExecuteAsync_WhenCancelsBetweenRetries_Throws()
    {
        using var cts = new CancellationTokenSource();
        var attempt = 0;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await _dataSource.ExecuteAsync(async (_, _) =>
            {
                attempt++;
                if (attempt == 1)
                {
                    await cts.CancelAsync();
                    throw new YdbException(StatusCode.BadSession, "Bad");
                }
            }, cts.Token);
        });

        Assert.Equal(1, attempt);
    }

    [Theory]
    [InlineData(10)]
    [InlineData(20)]
    [InlineData(30)]
    public async Task ExecuteInTransactionAsync_WhenTLI_ThenRetriesUntilSuccess(int concurrentJob)
    {
        var tableName = $"Table_TLI_{Random.Shared.Next()}";
        await using (var ydbConnection = await CreateOpenConnectionAsync())
        {
            await new YdbCommand(ydbConnection)
            {
                CommandText = $"CREATE TABLE {tableName} (id Int32, count Int32, PRIMARY KEY (id));"
            }.ExecuteNonQueryAsync();

            await new YdbCommand(ydbConnection)
                { CommandText = $"INSERT INTO {tableName} (id, count) VALUES (1, 0);" }.ExecuteNonQueryAsync();
        }

        var tasks = new List<Task>();
        for (var i = 0; i < concurrentJob; i++)
        {
            tasks.Add(_dataSource.ExecuteInTransactionAsync(async ydbConnection =>
            {
                var count = (int)(await new YdbCommand(ydbConnection)
                    { CommandText = $"SELECT count FROM {tableName} WHERE id = 1" }.ExecuteScalarAsync())!;

                await new YdbCommand(ydbConnection)
                {
                    CommandText = $"UPDATE {tableName} SET count = @count + 1 WHERE id = 1",
                    Parameters = { new YdbParameter { Value = count, ParameterName = "count" } }
                }.ExecuteNonQueryAsync();
            }, new YdbRetryPolicyConfig { MaxAttempts = concurrentJob }));
        }

        await Task.WhenAll(tasks);

        await using (var ydbConnection = await CreateOpenConnectionAsync())
        {
            Assert.Equal(concurrentJob, await new YdbCommand(ydbConnection)
                { CommandText = $"SELECT count FROM {tableName} WHERE id = 1" }.ExecuteScalarAsync());

            await new YdbCommand(ydbConnection) { CommandText = $"DROP TABLE {tableName}" }.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task RetryableConnection_WhenOpenTransaction_Throws()
    {
        await using var ydbConnection = await _dataSource.OpenRetryableConnectionAsync();
        await using var transaction = ydbConnection.BeginTransaction();

        Assert.Equal("Transactions are not supported in retryable session",
            (await Assert.ThrowsAsync<YdbException>(async () => await new YdbCommand(ydbConnection)
                { CommandText = "SELECT 1" }.ExecuteScalarAsync())).Message);
    }

    [Fact]
    public Task DescribeTable_ReturnsTableInfo() => RunTestWithTemporaryTable(
        """
        CREATE TABLE `{0}` (
            `field_1` Int32,
            `field_2` Int64,
            `field_3` String,
            `field_4` Text,
            `field_5` Datetime64,
            `field_6` Decimal(33, 0),
            `field_7` Bool,
            PRIMARY KEY (`field_1`, `field_2`),
            INDEX idx_1 GLOBAL UNIQUE ON (`field_3`, `field_4`),
            INDEX idx_2 GLOBAL ON (`field_5`, `field_6`, `field_7`) COVER (`field_3`, `field_4`),
            INDEX idx_3 GLOBAL ASYNC ON (`field_1`, `field_7`) COVER (`field_5`)
        )
        """, $"describe_raw_table_{Random.Shared.Next()});", async (_, tableName) =>
        {
            var tableDescription = await _dataSource.DescribeTable(tableName);
            Assert.Equal(tableName, tableDescription.Name);
            Assert.Equal(YdbTableType.Raw, tableDescription.Type);
            Assert.False(tableDescription.IsSystem);
            Assert.Equal(["field_1", "field_2"], tableDescription.PrimaryKey);
            Assert.Equal(YdbIndexType.GlobalUnique, tableDescription.Indexes[0].Type);
            Assert.Equal(YdbIndexType.Global, tableDescription.Indexes[1].Type);
            Assert.Equal(YdbIndexType.GlobalAsync, tableDescription.Indexes[2].Type);
            Assert.Equal(["field_3", "field_4"], tableDescription.Indexes[0].Columns);
            Assert.Equal("idx_1", tableDescription.Indexes[0].Name);
            Assert.Equal(["field_5", "field_6", "field_7"], tableDescription.Indexes[1].Columns);
            Assert.Equal("idx_2", tableDescription.Indexes[1].Name);
            Assert.Equal(["field_1", "field_7"], tableDescription.Indexes[2].Columns);
            Assert.Equal(["field_3", "field_4"], tableDescription.Indexes[1].CoverColumns);
            Assert.Equal("idx_3", tableDescription.Indexes[2].Name);
            Assert.Equal(["field_5"], tableDescription.Indexes[2].CoverColumns);
            Assert.Null(tableDescription.TableStats);

            var tableDescriptionWithStats =
                await _dataSource.DescribeTable(tableName, new DescribeTableSettings { IncludeTableStats = true });
            Assert.Equal(0ul, tableDescriptionWithStats.TableStats!.RowsEstimate);
        });

    [Fact]
    public Task DescribeColumnTable_ReturnsTableInfo() => RunTestWithTemporaryTable(
        """
        CREATE TABLE `{0}` (
            `field_1` Decimal(33, 0) NOT NULL,
            PRIMARY KEY (`field_1`)
        ) WITH (STORE = COLUMN)
        """, $"describe_column_table_{Random.Shared.Next()});", async (_, tableName) =>
        {
            var tableDescription = await _dataSource.DescribeTable(tableName);
            Assert.Equal(tableName, tableDescription.Name);
            Assert.Equal(YdbTableType.Column, tableDescription.Type);
            Assert.Single(tableDescription.Columns);
            Assert.Equal(YdbDbType.Decimal, tableDescription.Columns[0].StorageType.YdbDbType);
            Assert.Equal(33, tableDescription.Columns[0].StorageType.Precision);
            Assert.Equal(0, tableDescription.Columns[0].StorageType.Scale);
            Assert.Equal("field_1", tableDescription.Columns[0].Name);
            Assert.False(tableDescription.Columns[0].IsNullable);
            Assert.False(tableDescription.IsSystem);
            Assert.Equal(["field_1"], tableDescription.PrimaryKey);
            Assert.Empty(tableDescription.Indexes);
            Assert.Null(tableDescription.TableStats);
        }
    );

    [Fact]
    public Task CopyTable_SuccessCopied() => RunTestWithTemporaryTable(
        """
        CREATE TABLE `{0}` (
            id Serial,
            name Text,
            PRIMARY KEY (id)
        );
        """, $"copy_table_{Random.Shared.Next()}", async (ydbConnection, sourceTable) =>
        {
            const ulong size = 1000;
            var names = new List<string>();
            for (ulong i = 0; i < size; i++)
                names.Add($"name_{i}");

            var destinationTable = $"{sourceTable}_destination";

            await new YdbCommand($"INSERT INTO `{sourceTable}`" +
                                 "SELECT name FROM AS_TABLE(ListMap($list, ($x) -> { RETURN <|name: $x|> }))",
                ydbConnection) { Parameters = { new YdbParameter("list", names) } }.ExecuteNonQueryAsync();

            var countSource = new YdbCommand($"SELECT COUNT(*) FROM {sourceTable}", ydbConnection);
            var countDestination = new YdbCommand($"SELECT COUNT(*) FROM {destinationTable}", ydbConnection);

            Assert.Equal(size, await countSource.ExecuteScalarAsync());

            await _dataSource.CopyTable(sourceTable, destinationTable);

            Assert.Equal(size, await countSource.ExecuteScalarAsync());
            Assert.Equal(size, await countDestination.ExecuteScalarAsync());

            await _dataSource.DropTable(destinationTable);
        }
    );

    [Fact]
    public Task CopyTables_SuccessCopied() => RunTestWithTemporaryTable(
        """
        CREATE TABLE `{0}` (
            id Serial,
            name Text,
            INDEX idx_1 GLOBAL UNIQUE ON (`name`),
            PRIMARY KEY (id)
        );
        CREATE TABLE `{0}_another` (
            id Serial,
            name Text,
            INDEX idx_1 GLOBAL UNIQUE ON (`name`),
            PRIMARY KEY (id)
        );
        """, $"copy_table_{Random.Shared.Next()}", async (ydbConnection, sourceTable) =>
        {
            const ulong size = 1000;
            var names = new List<string>();
            for (ulong i = 0; i < size; i++)
                names.Add($"name_{i}");

            var destinationTable1 = $"{sourceTable}_destination1";
            var destinationTable2 = $"{sourceTable}_destination2";

            await new YdbCommand($"INSERT INTO `{sourceTable}`" +
                                 "SELECT name FROM AS_TABLE(ListMap($list, ($x) -> { RETURN <|name: $x|> }));" +
                                 $"INSERT INTO `{sourceTable}_another`" +
                                 "SELECT name FROM AS_TABLE(ListMap($list, ($x) -> { RETURN <|name: $x|> }));",
                ydbConnection) { Parameters = { new YdbParameter("list", names) } }.ExecuteNonQueryAsync();

            var countSource = new YdbCommand($"SELECT COUNT(*) FROM {sourceTable}", ydbConnection);
            var countDestination1 = new YdbCommand($"SELECT COUNT(*) FROM {destinationTable1}", ydbConnection);
            var countDestination2 = new YdbCommand($"SELECT COUNT(*) FROM {destinationTable2}", ydbConnection);

            Assert.Equal(size, await countSource.ExecuteScalarAsync());

            await _dataSource.CopyTables([
                new CopyTableSettings(SourceTable: sourceTable, DestinationTable: destinationTable1),
                new CopyTableSettings(SourceTable: $"{sourceTable}_another", DestinationTable: destinationTable2, true)
            ]);

            Assert.Equal(size, await countSource.ExecuteScalarAsync());
            Assert.Equal(size, await countDestination1.ExecuteScalarAsync());
            Assert.Equal(size, await countDestination2.ExecuteScalarAsync());

            var describeTable1 = await _dataSource.DescribeTable(destinationTable1);
            var describeTable2 = await _dataSource.DescribeTable(destinationTable2);

            Assert.NotEmpty(describeTable1.Indexes);
            Assert.Empty(describeTable2.Indexes);

            await _dataSource.DropTable($"{sourceTable}_another");
            await _dataSource.DropTable(destinationTable1);
            await _dataSource.DropTable(destinationTable2);
        }
    );

    [Fact]
    public Task RenameTables_SuccessRenamed() => RunTestWithTemporaryTable(
        """
        CREATE TABLE `{0}_old` (
            id Serial,
            name Text,
            PRIMARY KEY (id)
        );
        """, $"rename_table_{Random.Shared.Next()}",
        async (_, sourceTable) =>
            await _dataSource.RenameTables([new RenameTableSettings($"{sourceTable}_old", sourceTable)])
    );

    [Fact]
    public async Task CreateTable_SuccessCreated()
    {
        var tableName = $"create_table_{Random.Shared.Next()}";
        var tableDescription = new YdbTableDescription(tableName, new List<YdbColumnDescription>
        {
            new("field_1", YdbDbType.Datetime64),
            new("field_2", YdbDbType.Int32),
            new("field_3", YdbDbType.Int64) { IsNullable = false },
            new("field_4", YdbDbType.Text) { IsNullable = false },
            new("field_5", new YdbColumnType(YdbDbType.Decimal, 10, 3))
        }, new RepeatedField<string> { "field_1" })
        {
            Indexes = new List<YdbIndexDescription>
            {
                new("idx_1", YdbIndexType.GlobalUnique, new RepeatedField<string> { "field_2", "field_3" })
                    { CoverColumns = new RepeatedField<string>() }
            }
        };

        await _dataSource.CreateTable(tableDescription);
        var describeTable = await _dataSource.DescribeTable(tableName);

        Assert.Equal(tableName, describeTable.Name);
        Assert.Equal(YdbTableType.Raw, describeTable.Type);
        Assert.False(describeTable.IsSystem);
        Assert.Equal(["field_1"], describeTable.PrimaryKey);
        Assert.Equal(YdbIndexType.GlobalUnique, describeTable.Indexes[0].Type);
        Assert.Single(describeTable.Indexes);
        Assert.Equal(["field_2", "field_3"], describeTable.Indexes[0].Columns);
        Assert.Equal("idx_1", describeTable.Indexes[0].Name);
        Assert.Empty(describeTable.Indexes[0].CoverColumns);
        Assert.Equal(5, describeTable.Columns.Count);
        Assert.Equal("field_1", describeTable.Columns[0].Name);
        Assert.Equal(YdbDbType.Datetime64, describeTable.Columns[0].StorageType.YdbDbType);
        Assert.True(describeTable.Columns[0].IsNullable);

        Assert.Equal("field_3", describeTable.Columns[2].Name);
        Assert.Equal(YdbDbType.Int64, describeTable.Columns[2].StorageType.YdbDbType);
        Assert.False(describeTable.Columns[2].IsNullable);

        Assert.Equal("field_5", describeTable.Columns[4].Name);
        Assert.Equal(YdbDbType.Decimal, describeTable.Columns[4].StorageType.YdbDbType);
        Assert.Equal(10, describeTable.Columns[4].StorageType.Precision);
        Assert.Equal(3, describeTable.Columns[4].StorageType.Scale);
        Assert.True(describeTable.Columns[4].IsNullable);

        await _dataSource.DropTable(tableName);
    }

    // [Fact] https://github.com/ydb-platform/ydb/issues/31123
    // public Task RenameTablesWithReplace_SuccessRenamed() => RunTestWithTemporaryTable(
    //     """
    //     CREATE TABLE `{0}_old` (
    //         id Serial,
    //         name Text,
    //         PRIMARY KEY (id)
    //     );
    //     CREATE TABLE `{0}` (
    //         id Serial,
    //         new_name Text,
    //         PRIMARY KEY (id)
    //     );
    //     """, $"rename_table_{Random.Shared.Next()}",
    //     async (_, sourceTable) =>
    //     {
    //         await _dataSource.RenameTables([new RenameTableSettings($"{sourceTable}_old", sourceTable, true)]);
    //         var describeTable = await _dataSource.DescribeTable(sourceTable);
    //         Assert.Contains(describeTable.Columns, collection => collection.Name.Equals("name"));
    //     });
}
