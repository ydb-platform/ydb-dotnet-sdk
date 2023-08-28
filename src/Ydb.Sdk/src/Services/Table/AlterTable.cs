using Ydb.Sdk.Client;
using Ydb.Sdk.Operations;

namespace Ydb.Sdk.Table
{
    public class IndexBuildMetadata
    {
        internal static IndexBuildMetadata FromProto(Ydb.Table.IndexBuildMetadata metaProto)
        {
            return new IndexBuildMetadata();
        }
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

        internal static AlterTableMetadata FromProto(Google.Protobuf.WellKnownTypes.Any metaProto)
        {
            if (metaProto.Is(Ydb.Table.IndexBuildMetadata.Descriptor))
            {
                return new AlterTableMetadata(metaProto.Unpack<Ydb.Table.IndexBuildMetadata>());
            }


            return new AlterTableMetadata();
        }
    }

    public sealed class AlterTableOperation : OperationResponse<EmptyResult, AlterTableMetadata>
    {
        private OperationsClient _operationsClient;

        internal AlterTableOperation(OperationsClient operationsClient, ClientOperation operation)
            : base(operation)
        {
            _operationsClient = operationsClient;
        }

        internal AlterTableOperation(OperationsClient operationsClient, Status status)
            : this(operationsClient, new ClientOperation(status))
        {
        }

        protected override EmptyResult UnpackResult(ClientOperation operation)
        {
            return new EmptyResult();
        }

        protected override AlterTableMetadata UnpackMetadata(ClientOperation operation)
        {
            return AlterTableMetadata.FromProto(operation.Metadata);
        }

        public async Task<AlterTableOperation> Poll()
        {
            return new AlterTableOperation(_operationsClient, await _operationsClient.GetOperation(Id));
        }
        public async Task<AlterTableOperation> PollReady(TimeSpan? delay = default,
            CancellationToken cancellationToken = default)
        {
            return new AlterTableOperation(_operationsClient, await _operationsClient.PollReady(Id, delay, cancellationToken));
        }
    }

    public class AddIndexSettings : OperationRequestSettings
    {
        public string Name { get; set; } = String.Empty;
        public List<string> IndexColumns { get; set; } = new List<string>();
    }

    public partial class TableClient
    {
        public async Task<AlterTableOperation> AddIndex(string tablePath, AddIndexSettings? settings = null)
        {
            settings ??= new AddIndexSettings();

            var request = new Ydb.Table.AlterTableRequest
            {
                OperationParams = MakeOperationParams(settings),
                Path = tablePath,
                AddIndexes = {
                    new Ydb.Table.TableIndex {
                        Name = settings.Name,
                        IndexColumns = { settings.IndexColumns },
                        GlobalIndex = new Ydb.Table.GlobalIndex()
                    } 
                }
            };

            try
            {
                var response = await Driver.UnaryCall(
                    method: Ydb.Table.V1.TableService.AlterTableMethod,
                    request: request,
                    settings: settings);

                return new AlterTableOperation(
                    new OperationsClient(Driver), 
                    ClientOperation.FromProto(response.Data.Operation));
            }
            catch (Driver.TransportException e)
            {
                return new AlterTableOperation(new OperationsClient(Driver), e.Status);
            }
        }
    }
}
