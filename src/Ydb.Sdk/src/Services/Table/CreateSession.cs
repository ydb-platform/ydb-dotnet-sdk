using System.Web;
using Ydb.Sdk.Client;
using Ydb.Sdk.Services.Operations;
using Ydb.Table;
using Ydb.Table.V1;

namespace Ydb.Sdk.Services.Table;

public class CreateSessionSettings : OperationSettings
{
}

public class CreateSessionResponse : ResponseWithResultBase<CreateSessionResponse.ResultData>
{
    internal CreateSessionResponse(Status status, ResultData? result = null)
        : base(status, result)
    {
    }

    public class ResultData
    {
        private ResultData(Session session)
        {
            Session = session;
        }

        public Session Session { get; }

        internal static ResultData FromProto(CreateSessionResult resultProto, Driver driver)
        {
            var session = new Session(
                driver: driver,
                sessionPool: null,
                id: resultProto.SessionId,
                nodeId: long.Parse(HttpUtility.ParseQueryString(new Uri(resultProto.SessionId).Query)["node_id"] ?? "0")
            );

            return new ResultData(
                session: session
            );
        }
    }
}

public partial class TableClient
{
    public async Task<CreateSessionResponse> CreateSession(CreateSessionSettings? settings = null)
    {
        settings ??= new CreateSessionSettings();

        var request = new CreateSessionRequest
        {
            OperationParams = settings.MakeOperationParams()
        };

        try
        {
            var response = await _driver.UnaryCall(
                method: TableService.CreateSessionMethod,
                request: request,
                settings: settings
            );

            var status = response.Operation.TryUnpack(out CreateSessionResult? resultProto);

            CreateSessionResponse.ResultData? result = null;
            if (status.IsSuccess && resultProto != null)
            {
                result = CreateSessionResponse.ResultData.FromProto(resultProto, _driver);
            }

            return new CreateSessionResponse(status, result);
        }
        catch (Driver.TransportException e)
        {
            return new CreateSessionResponse(e.Status);
        }
    }
}
