using Microsoft.Extensions.Logging;
using Ydb.Query;
using Ydb.Query.V1;
using Ydb.Sdk.Client;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query;

public abstract class QueryClientGrpc : ClientBase
{
    private protected QueryClientGrpc(Driver driver) : base(driver)
    {
        Driver.LoggerFactory.CreateLogger<QueryClient>();
    }

    internal async Task<CreateSessionResponse> CreateSession(CreateSessionSettings? settings = null)
    {
        settings ??= new CreateSessionSettings();
        var request = new CreateSessionRequest();

        try
        {
            var response = await Driver.UnaryCall(
                method: QueryService.CreateSessionMethod,
                request: request,
                settings: settings);

            var status = Status.FromProto(response.Data.Status, response.Data.Issues);

            CreateSessionResponse.ResultData? result = null;

            if (status.IsSuccess)
            {
                result = CreateSessionResponse.ResultData.FromProto(response.Data, Driver, response.UsedEndpoint);
            }

            return new CreateSessionResponse(status, result);
        }
        catch (Driver.TransportException e)
        {
            return new CreateSessionResponse(e.Status);
        }
    }

    internal async Task<DeleteSessionResponse> DeleteSession(string sessionId, DeleteSessionSettings? settings = null)
    {
        settings ??= new DeleteSessionSettings();
        var request = new DeleteSessionRequest
        {
            SessionId = sessionId
        };

        try
        {
            var response = await Driver.UnaryCall(
                method: QueryService.DeleteSessionMethod,
                request: request,
                settings: settings);


            return DeleteSessionResponse.FromProto(response.Data);
        }
        catch (Driver.TransportException e)
        {
            return new DeleteSessionResponse(e.Status);
        }
    }

    internal SessionStateStream AttachSession(string sessionId, AttachSessionSettings? settings = null)
    {
        settings ??= new AttachSessionSettings { TransportTimeout = TimeSpan.FromDays(1) };

        var request = new AttachSessionRequest { SessionId = sessionId };

        var streamIterator = Driver.StreamCall(
            method: QueryService.AttachSessionMethod,
            request: request,
            settings: settings
        );
        return new SessionStateStream(streamIterator);
    }

    private protected async Task<BeginTransactionResponse> BeginTransaction(
        string sessionId,
        Tx tx,
        BeginTransactionSettings? settings = null)
    {
        settings ??= new BeginTransactionSettings();

        var request = new BeginTransactionRequest { SessionId = sessionId, TxSettings = tx.ToProto().BeginTx };
        try
        {
            var response = await Driver.UnaryCall(
                QueryService.BeginTransactionMethod,
                request: request,
                settings: settings
            );
            return BeginTransactionResponse.FromProto(response.Data, this, sessionId);
        }
        catch (Driver.TransportException e)
        {
            return new BeginTransactionResponse(e.Status);
        }
    }

    private protected async Task<CommitTransactionResponse> CommitTransaction(
        string sessionId,
        Tx tx,
        CommitTransactionSettings? settings = null)
    {
        settings ??= new CommitTransactionSettings();

        var request = new CommitTransactionRequest { SessionId = sessionId, TxId = tx.TxId };

        try
        {
            var response = await Driver.UnaryCall(
                QueryService.CommitTransactionMethod,
                request: request,
                settings: settings
            );
            return CommitTransactionResponse.FromProto(response.Data);
        }
        catch (Driver.TransportException e)
        {
            return new CommitTransactionResponse(e.Status);
        }
    }

    private protected async Task<RollbackTransactionResponse> RollbackTransaction(
        string sessionId,
        Tx tx,
        RollbackTransactionSettings? settings = null)
    {
        settings ??= new RollbackTransactionSettings();

        var request = new RollbackTransactionRequest { SessionId = sessionId, TxId = tx.TxId };
        try
        {
            var response = await Driver.UnaryCall(
                QueryService.RollbackTransactionMethod,
                request: request,
                settings: settings
            );
            return RollbackTransactionResponse.FromProto(response.Data);
        }
        catch (Driver.TransportException e)
        {
            return new RollbackTransactionResponse(e.Status);
        }
    }


    protected internal ExecuteQueryStream ExecuteQuery(
        string sessionId,
        string queryString,
        Tx tx,
        IReadOnlyDictionary<string, YdbValue>? parameters,
        ExecuteQuerySettings? settings = null)
    {
        settings ??= new ExecuteQuerySettings();
        parameters ??= new Dictionary<string, YdbValue>();

        var request = new ExecuteQueryRequest
        {
            SessionId = sessionId,
            ExecMode = (Ydb.Query.ExecMode)settings.ExecMode,
            TxControl = tx.ToProto(),
            QueryContent = new QueryContent { Syntax = (Ydb.Query.Syntax)settings.Syntax, Text = queryString },
            StatsMode = (Ydb.Query.StatsMode)settings.StatsMode
        };

        request.Parameters.Add(parameters.ToDictionary(p => p.Key, p => p.Value.GetProto()));

        var streamIterator = Driver.StreamCall(
            method: QueryService.ExecuteQueryMethod,
            request: request,
            settings: settings);

        return new ExecuteQueryStream(streamIterator);
    }
}
