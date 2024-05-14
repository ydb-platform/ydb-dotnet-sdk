using Microsoft.Extensions.Logging;
using Ydb.Query;
using Ydb.Query.V1;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query;

internal class QueryClientRpc
{
    private readonly Driver _driver;

    internal QueryClientRpc(Driver driver)
    {
        _driver = driver;
        _driver.LoggerFactory.CreateLogger<QueryClient>();
    }

    internal async Task<CreateSessionResponse> CreateSession(SessionPool sessionPool, CreateSessionSettings? settings = null)
    {
        settings ??= new CreateSessionSettings();
        var request = new CreateSessionRequest();

        try
        {
            var response = await _driver.UnaryCall(
                method: QueryService.CreateSessionMethod,
                request: request,
                settings: settings);

            var status = Status.FromProto(response.Data.Status, response.Data.Issues);

            CreateSessionResponse.ResultData? result = null;

            if (status.IsSuccess)
            {
                result = CreateSessionResponse.ResultData.FromProto(sessionPool, response.Data,  _driver, response.UsedEndpoint);
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
            var response = await _driver.UnaryCall(
                method: QueryService.DeleteSessionMethod,
                request: request,
                settings: settings
            );

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

        var streamIterator = _driver.StreamCall(
            method: QueryService.AttachSessionMethod,
            request: request,
            settings: settings
        );
        return new SessionStateStream(streamIterator);
    }

    internal async Task<BeginTransactionResponse> BeginTransaction(
        string sessionId,
        Tx tx,
        BeginTransactionSettings? settings = null)
    {
        settings ??= new BeginTransactionSettings();

        var request = new BeginTransactionRequest
            { SessionId = sessionId, TxSettings = tx.TxMode.TransactionSettings() };
        try
        {
            var response = await _driver.UnaryCall(
                QueryService.BeginTransactionMethod,
                request: request,
                settings: settings
            );

            return BeginTransactionResponse.FromProto(response.Data);
        }
        catch (Driver.TransportException e)
        {
            return new BeginTransactionResponse(e.Status);
        }
    }

    internal async Task<CommitTransactionResponse> CommitTransaction(
        string sessionId,
        Tx tx,
        CommitTransactionSettings? settings = null)
    {
        settings ??= new CommitTransactionSettings();

        var request = new CommitTransactionRequest { SessionId = sessionId, TxId = tx.TxId };

        try
        {
            var response = await _driver.UnaryCall(
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

    internal async Task<RollbackTransactionResponse> RollbackTransaction(
        string sessionId,
        Tx tx,
        RollbackTransactionSettings? settings = null)
    {
        settings ??= new RollbackTransactionSettings();

        var request = new RollbackTransactionRequest { SessionId = sessionId, TxId = tx.TxId };
        try
        {
            var response = await _driver.UnaryCall(
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
        string query,
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
            QueryContent = new QueryContent { Syntax = (Ydb.Query.Syntax)settings.Syntax, Text = query },
            StatsMode = (Ydb.Query.StatsMode)settings.StatsMode
        };

        if (tx.TxMode != TxMode.None)
        {
            request.TxControl = new TransactionControl
            {
                BeginTx = tx.TxMode.TransactionSettings(),
                CommitTx = tx.AutoCommit
            };
        }

        request.Parameters.Add(parameters.ToDictionary(p => p.Key, p => p.Value.GetProto()));

        var streamIterator = _driver.StreamCall(
            method: QueryService.ExecuteQueryMethod,
            request: request,
            settings: settings);

        return new ExecuteQueryStream(streamIterator);
    }
}
