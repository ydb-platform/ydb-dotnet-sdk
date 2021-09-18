using System;
using System.Threading.Tasks;
using Ydb.Sdk.Client;

namespace Ydb.Sdk.Table
{
    public class KeepAliveSettings : OperationRequestSettings
    {
    }

    public class KeepAliveResponse : ResponseWithResultBase<KeepAliveResponse.ResultData>
    {
        internal KeepAliveResponse(Status status, ResultData? result = null)
            : base(status, result)
        {
        }

        public enum SessionStatus : uint
        {
            Unspecified = 0,
            Ready = 1,
            Busy = 2
        }

        public class ResultData
        {
            internal ResultData(SessionStatus sessionStatus)
            {
                SessionStatus = sessionStatus;
            }

            public SessionStatus SessionStatus { get; }

            internal static ResultData FromProto(Ydb.Table.KeepAliveResult resultProto)
            {
                return new ResultData (
                    sessionStatus: SessionStatusFromProto(resultProto.SessionStatus)
                );
            }
        }

        static private SessionStatus SessionStatusFromProto(Ydb.Table.KeepAliveResult.Types.SessionStatus proto)
        {
            uint value = (uint)proto;
            if (Enum.IsDefined(typeof(SessionStatus), value))
            {
                return (SessionStatus)value;
            }

            return SessionStatus.Unspecified;
        }
    }

    public partial class TableClient
    {
        internal async Task<KeepAliveResponse> KeepAlive(string sessionId, KeepAliveSettings? settings = null)
        {
            settings ??= new KeepAliveSettings();

            var request = new Ydb.Table.KeepAliveRequest
            {
                OperationParams = MakeOperationParams(settings),
                SessionId = sessionId
            };

            try
            {
                var response = await Driver.UnaryCall(
                    method: Ydb.Table.V1.TableService.KeepAliveMethod,
                    request: request,
                    settings: settings);

                Ydb.Table.KeepAliveResult? resultProto;
                var status = UnpackOperation(response.Data.Operation, out resultProto);

                KeepAliveResponse.ResultData? result = null;
                if (status.IsSuccess && resultProto != null)
                {
                    result = KeepAliveResponse.ResultData.FromProto(resultProto);
                }

                return new KeepAliveResponse(status, result);
            }
            catch (Driver.TransportException e)
            {
                return new KeepAliveResponse(e.Status);
            }
        }
    }
}
