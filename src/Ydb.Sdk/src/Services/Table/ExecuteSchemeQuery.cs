using Ydb.Sdk.Client;
using Ydb.Sdk.Services.Operations;
using Ydb.Table;
using Ydb.Table.V1;

namespace Ydb.Sdk.Services.Table;

public class ExecuteSchemeQuerySettings : OperationSettings
{
}

public class ExecuteSchemeQueryResponse : ResponseBase
{
    internal ExecuteSchemeQueryResponse(Status status)
        : base(status)
    {
    }
}
