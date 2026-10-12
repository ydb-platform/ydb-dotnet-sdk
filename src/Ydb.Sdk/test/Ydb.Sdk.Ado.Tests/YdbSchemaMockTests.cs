using System.Data;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Moq;
using Xunit;
using Ydb.Scheme;
using Ydb.Scheme.V1;
using Ydb.Sdk.Ado.Session;
using Ydb.Sdk.Ado.Tests.Utils;
using Ydb.Table;
using Ydb.Table.V1;

namespace Ydb.Sdk.Ado.Tests;

/// <summary>
/// Schema collections against a mocked driver, so that a table can deterministically disappear
/// between the directory listing and the describe call.
/// </summary>
public class YdbSchemaMockTests
{
    private const string Database = "/local";
    private const string DroppedTable = "dropped_table";
    private const string ExistingTable = "existing_table";

    [Theory]
    [InlineData("Columns")]
    [InlineData("TablesWithStats")]
    public async Task GetSchema_WhenListedTableIsDroppedBeforeDescribe_SkipsDroppedTable(string collectionName)
    {
        var driver = CreateDriver(StatusIds.Types.StatusCode.SchemeError);

        var dataTable = await GetSchemaAsync(driver.Object, collectionName, [null, null]);

        var tableNames = dataTable.Rows.Cast<DataRow>().Select(row => row["table_name"]).Distinct().ToList();
        Assert.Equal([ExistingTable], tableNames);
        driver.Verify(d => d.UnaryCall(
            TableService.DescribeTableMethod,
            It.Is<DescribeTableRequest>(request => request.Path == $"{Database}/{DroppedTable}"),
            It.IsAny<GrpcRequestSettings>()), Times.Once());
    }

    [Theory]
    [InlineData("Columns")]
    [InlineData("TablesWithStats")]
    public async Task GetSchema_WhenDescribeOfListedTableFailsWithOtherError_Throws(string collectionName)
    {
        var driver = CreateDriver(StatusIds.Types.StatusCode.Unauthorized);

        var exception = await Assert.ThrowsAsync<YdbException>(() =>
            GetSchemaAsync(driver.Object, collectionName, [null, null]));

        Assert.Equal(StatusCode.Unauthorized, exception.Code);
    }

    [Theory]
    [InlineData("Columns")]
    [InlineData("TablesWithStats")]
    public async Task GetSchema_WhenRestrictedToDroppedTable_Throws(string collectionName)
    {
        var driver = CreateDriver(StatusIds.Types.StatusCode.SchemeError);

        var exception = await Assert.ThrowsAsync<YdbException>(() =>
            GetSchemaAsync(driver.Object, collectionName, [DroppedTable, null]));

        Assert.Equal(StatusCode.SchemeError, exception.Code);
    }

    private static async Task<DataTable> GetSchemaAsync(
        IDriver driver,
        string collectionName,
        string?[] restrictions)
    {
        var settings = new YdbConnectionStringBuilder
        {
            Database = Database,
            PoolName = $"schema-mock-{Guid.NewGuid()}",
            LoggerFactory = TestUtils.LoggerFactory
        };

        await using var source = new ImplicitSessionSource(driver, settings);
        Assert.True(PoolManager.Pools.TryAdd(settings.PoolKey, source));
        try
        {
            await using var connection = new YdbConnection(settings);
            await connection.OpenAsync();

            return await connection.GetSchemaAsync(collectionName, restrictions);
        }
        finally
        {
            Assert.True(PoolManager.Pools.TryRemove(settings.PoolKey, out _));
        }
    }

    private static Mock<IDriver> CreateDriver(StatusIds.Types.StatusCode droppedTableStatus)
    {
        var driver = new Mock<IDriver>(MockBehavior.Strict);
        driver.Setup(d => d.Database).Returns(Database);
        driver.Setup(d => d.DisposeAsync()).Returns(ValueTask.CompletedTask);

        driver.Setup(d => d.UnaryCall(
                SchemeService.ListDirectoryMethod,
                It.IsAny<ListDirectoryRequest>(),
                It.IsAny<GrpcRequestSettings>()))
            .ReturnsAsync(new ListDirectoryResponse
            {
                Operation = SuccessOperation(new ListDirectoryResult
                {
                    Children =
                    {
                        new Entry { Name = DroppedTable, Type = Entry.Types.Type.Table },
                        new Entry { Name = ExistingTable, Type = Entry.Types.Type.Table }
                    }
                })
            });

        driver.Setup(d => d.UnaryCall(
                TableService.DescribeTableMethod,
                It.Is<DescribeTableRequest>(request => request.Path == $"{Database}/{DroppedTable}"),
                It.IsAny<GrpcRequestSettings>()))
            .ReturnsAsync(new DescribeTableResponse
            {
                // The server reports a dropped path as SchemeError without any issues.
                Operation = new Operations.Operation { Ready = true, Status = droppedTableStatus }
            });

        driver.Setup(d => d.UnaryCall(
                TableService.DescribeTableMethod,
                It.Is<DescribeTableRequest>(request => request.Path == $"{Database}/{ExistingTable}"),
                It.IsAny<GrpcRequestSettings>()))
            .ReturnsAsync(new DescribeTableResponse
            {
                Operation = SuccessOperation(new DescribeTableResult
                {
                    Self = new Entry { Name = ExistingTable, Type = Entry.Types.Type.Table },
                    Columns =
                    {
                        new ColumnMeta
                        {
                            Name = "a",
                            Type = new Type { TypeId = Type.Types.PrimitiveTypeId.Int32 }
                        }
                    },
                    PrimaryKey = { "a" },
                    TableStats = new Table.TableStats { CreationTime = Timestamp.FromDateTime(DateTime.UnixEpoch) }
                })
            });

        return driver;
    }

    private static Operations.Operation SuccessOperation(IMessage result) =>
        new()
        {
            Ready = true,
            Status = StatusIds.Types.StatusCode.Success,
            Result = Any.Pack(result)
        };
}
