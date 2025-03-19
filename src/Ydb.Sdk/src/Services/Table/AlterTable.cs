using Google.Protobuf.WellKnownTypes;
using Ydb.Sdk.Client;
using Ydb.Sdk.Services.Operations;
using Ydb.Table;
using Ydb.Table.V1;

namespace Ydb.Sdk.Services.Table;

public class IndexBuildMetadata
{
    internal static IndexBuildMetadata FromProto(Ydb.Table.IndexBuildMetadata metaProto) => new();
}

public class AlterTableMetadata
{
    private AlterTableMetadata(Ydb.Table.IndexBuildMetadata indexBuildMeta)
    {
        IndexBuild = IndexBuildMetadata.FromProto(indexBuildMeta);
    }

    private AlterTableMetadata()
    {
    }

    public IndexBuildMetadata? IndexBuild { get; }

    internal static AlterTableMetadata FromProto(Any metaProto) =>
        metaProto.Is(Ydb.Table.IndexBuildMetadata.Descriptor)
            ? new AlterTableMetadata(metaProto.Unpack<Ydb.Table.IndexBuildMetadata>())
            : new AlterTableMetadata();
}

public sealed class AlterTableOperation : OperationResponse<EmptyResult, AlterTableMetadata>
{
    private readonly OperationsClient _operationsClient;

    internal AlterTableOperation(OperationsClient operationsClient, ClientOperation operation)
        : base(operation)
    {
        _operationsClient = operationsClient;
    }

    internal AlterTableOperation(OperationsClient operationsClient, Status status)
        : this(operationsClient, new ClientOperation(status))
    {
    }

    protected override EmptyResult UnpackResult(ClientOperation operation) => new();

    protected override AlterTableMetadata UnpackMetadata(ClientOperation operation) =>
        AlterTableMetadata.FromProto(operation.Metadata);

    public async Task<AlterTableOperation> Poll() => new(_operationsClient, await _operationsClient.GetOperation(Id));

    public async Task<AlterTableOperation> PollReady(TimeSpan? delay = default,
        CancellationToken cancellationToken = default) =>
        new(_operationsClient,
            await _operationsClient.PollReady(Id, delay, cancellationToken));
}

public class AddIndexSettings : OperationSettings
{
    public string Name { get; set; } = string.Empty;
    public List<string> IndexColumns { get; set; } = new();
}

public partial class TableClient
{
    public async Task<AlterTableOperation> AddIndex(string tablePath, AddIndexSettings? settings = null)
    {
        settings ??= new AddIndexSettings();

        var request = new AlterTableRequest
        {
            OperationParams = settings.MakeOperationParams(),
            Path = tablePath,
            AddIndexes =
            {
                new TableIndex
                {
                    Name = settings.Name,
                    IndexColumns = { settings.IndexColumns },
                    GlobalIndex = new GlobalIndex()
                }
            }
        };

        try
        {
            var response = await _driver.UnaryCall(
                method: TableService.AlterTableMethod,
                request: request,
                settings: settings
            );

            return new AlterTableOperation(
                new OperationsClient(_driver),
                ClientOperation.FromProto(response.Operation)
            );
        }
        catch (Driver.TransportException e)
        {
            return new AlterTableOperation(new OperationsClient(_driver), e.Status);
        }
    }
}
