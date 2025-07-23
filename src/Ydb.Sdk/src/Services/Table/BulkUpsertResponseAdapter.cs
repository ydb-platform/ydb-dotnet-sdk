using Ydb.Sdk.Client;
using Ydb.Operations;
using Ydb.Table;

namespace Ydb.Sdk.Services.Table
{
    public class BulkUpsertResponseAdapter : IResponse
    {
        public Status Status { get; }
        public BulkUpsertResponse Response { get; }

        public BulkUpsertResponseAdapter(BulkUpsertResponse response)
        {
            Response = response ?? throw new ArgumentNullException(nameof(response));
            Status = Status.FromProto(response.Operation.Status, response.Operation.Issues);
        }
    }
}
