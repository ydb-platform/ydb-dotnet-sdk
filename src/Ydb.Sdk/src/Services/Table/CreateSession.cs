using Ydb.Sdk.Client;
using Ydb.Table;
using Ydb.Table.V1;

namespace Ydb.Sdk.Services.Table;

public class CreateSessionSettings : OperationRequestSettings
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
        internal ResultData(Session session)
        {
            Session = session;
        }

        public Session Session { get; }

        internal static ResultData FromProto(CreateSessionResult resultProto, Driver driver, string endpoint)
        {
            var session = new Session(
                driver: driver,
                sessionPool: null,
                id: resultProto.SessionId,
                endpoint: endpoint);

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
            OperationParams = MakeOperationParams(settings)
        };

        try
        {
            var response = await Driver.UnaryCall(
                method: TableService.CreateSessionMethod,
                request: request,
                settings: settings);

            var status = UnpackOperation(response.Data.Operation, out CreateSessionResult? resultProto);

            CreateSessionResponse.ResultData? result = null;
            if (status.IsSuccess && resultProto != null)
            {
                result = CreateSessionResponse.ResultData.FromProto(resultProto, Driver, response.UsedEndpoint);
            }

            return new CreateSessionResponse(status, result);
        }
        catch (Driver.TransportException e)
        {
            return new CreateSessionResponse(e.Status);
        }
    }
}