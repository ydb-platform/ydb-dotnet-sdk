using Ydb.Sdk.Client;

namespace Ydb.Sdk.Table
{
    public class ReadTableSettings : RequestSettings
    {
        public List<string> Columns { get; set; } = new List<string>();

        public UInt64 RowLimit { get; set; } = 0;

        public bool Ordered { get; set; } = false;
    }

    public class ReadTablePart : ResponseWithResultBase<ReadTablePart.ResultData>
    {
        internal ReadTablePart(Status status, ResultData? result = null)
            : base(status, result)
        {
        }

        public class ResultData
        {
            internal ResultData(Value.ResultSet resultSet)
            {
                ResultSet = resultSet;
            }

            public Value.ResultSet ResultSet { get; }

            internal static ResultData FromProto(Ydb.Table.ReadTableResult resultProto)
            {
                return new ResultData(
                    resultSet: Value.ResultSet.FromProto(resultProto.ResultSet)
                );
            }
        }
    }

    public class ReadTableStream : StreamResponse<Ydb.Table.ReadTableResponse, ReadTablePart>
    {
        internal ReadTableStream(Driver.StreamIterator<Ydb.Table.ReadTableResponse> iterator)
            : base(iterator)
        {
        }

        protected override ReadTablePart MakeResponse(Status status)
        {
            return new ReadTablePart(status);
        }

        protected override ReadTablePart MakeResponse(Ydb.Table.ReadTableResponse protoResponse)
        {
            var status = Status.FromProto(protoResponse.Status, protoResponse.Issues);
            var result = status.IsSuccess && protoResponse.Result != null
                ? ReadTablePart.ResultData.FromProto(protoResponse.Result)
                : null;

            return new ReadTablePart(status, result);
        }
    }

    public partial class TableClient
    {
        public ReadTableStream ReadTable(string tablePath, ReadTableSettings? settings = null)
        {
            settings ??= new ReadTableSettings();

            var request = new Ydb.Table.ReadTableRequest
            {
                Path = tablePath,
                Columns = { settings.Columns },
                RowLimit = settings.RowLimit,
                Ordered = settings.Ordered,
            };

            var streamIterator = Driver.StreamCall(
                method: Ydb.Table.V1.TableService.StreamReadTableMethod,
                request: request,
                settings: settings);

            return new ReadTableStream(streamIterator);
        }
    }
}
