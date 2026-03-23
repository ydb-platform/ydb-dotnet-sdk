using System.Collections.Concurrent;
using Google.Protobuf;
using Ydb.Coordination;
using Ydb.Coordination.V1;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Coordination.Impl;
using Ydb.Sdk.Coordination.Settings;

namespace Ydb.Sdk.Coordination;

public class CoordinationSession
{
    private readonly IDriver _driver;
    private readonly CoordinationNodeSettings _settings;
    //private readonly YdbRetryPolicyExecutor _ydbRetryPolicyExecutor;


    //private ulong _sessionId;
    private ulong _reqIdCounter;
    private ulong _seqNo;


    // Tasks for waiting SessionStarted and SessionStopped
    private TaskCompletionSource? _sessionStartedTcs;
    private TaskCompletionSource? _sessionStoppedTcs;

    // Bidirectional stream handler
    private IBidirectionalStream<SessionRequest, SessionResponse>? _stream;
    private readonly ConcurrentDictionary<ulong, PendingRequest<PendingResult>> _pendingRequests = new();
    //private readonly List<SessionRequest> _fireAndForgetRequests = new();

    // Reconnection state
    //private bool _closed = false;

    // Map of _reqIdCounter to semaphore name for watch tracking
    //private readonly ConcurrentDictionary<ulong, string> _watchedSemaphores = new();


    private readonly TaskCompletionSource _firstSessionStartedTcs =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    // EVENTS
    //public event Action<SessionExpiredEvent>? SessionExpired;
    //public event Action<SemaphoreChangedEvent>? SemaphoreChanged;    

    // private readonly CancellationTokenSource _disposeCts = new();

    public CoordinationSession(IDriver driver, CoordinationNodeSettings settings) //IRetryPolicy retryPolicy
    {
        //_ydbRetryPolicyExecutor = new YdbRetryPolicyExecutor(retryPolicy);
        _driver = driver;
        _settings = settings;
        Initialize();
    }


    private async void Initialize()
    {
        try
        {
            // _ydbRetryPolicyExecutor.ExecuteAsync();
            await StreamCall();
        }
        catch (Exception)
        {
            // Handle the exception (log it, rethrow it, etc.)
            // Console.WriteLine($"An error occurred: {ex.Message}");
        }
    }


    // простой пример создания stream, отправляем серверу сообщение о старте и получаем ответ от сервера
    private async Task StreamCall()
    {
        _stream = await _driver.BidirectionalStreamCall(CoordinationService.SessionMethod, _settings);
        await RunProcessingStreamResponse();
        await SendStartSession();
    }


    private async Task RunProcessingStreamResponse()
    {
        try
        {
            while (await _stream!.MoveNextAsync())
            {
                var response = _stream.Current;
                switch (response.ResponseCase)
                {
                    case SessionResponse.ResponseOneofCase.Ping:
                        // Server sent ping, respond with pong
                        await HandlePing(response);
                        break;
                    case SessionResponse.ResponseOneofCase.Failure:
                        await HandleFailure(response);
                        break;
                    case SessionResponse.ResponseOneofCase.SessionStarted:
                        HandleSessionStarted(); //HandleSessionStarted(response);
                        break;
                    case SessionResponse.ResponseOneofCase.SessionStopped:
                        HandleSessionStopped(); //HandleSessionStopped(response)
                        break;
                    case SessionResponse.ResponseOneofCase.AcquireSemaphorePending:
                        // note
                        break;
                    case SessionResponse.ResponseOneofCase.DescribeSemaphoreChanged:
                        // note
                        break;
                    case SessionResponse.ResponseOneofCase.None:
                    case SessionResponse.ResponseOneofCase.Pong:
                    case SessionResponse.ResponseOneofCase.Unsupported6:
                    case SessionResponse.ResponseOneofCase.Unsupported7:
                    case SessionResponse.ResponseOneofCase.AcquireSemaphoreResult:
                    case SessionResponse.ResponseOneofCase.ReleaseSemaphoreResult:
                    case SessionResponse.ResponseOneofCase.DescribeSemaphoreResult:
                    case SessionResponse.ResponseOneofCase.CreateSemaphoreResult:
                    case SessionResponse.ResponseOneofCase.UpdateSemaphoreResult:
                    case SessionResponse.ResponseOneofCase.DeleteSemaphoreResult:
                    case SessionResponse.ResponseOneofCase.Unsupported16:
                    case SessionResponse.ResponseOneofCase.Unsupported17:
                    case SessionResponse.ResponseOneofCase.Unsupported18:
                        break;
                }


                var reqId = ExtractReqId(response);
                if (reqId.HasValue &&
                    _pendingRequests.TryRemove(reqId.Value, out var pending))
                {
                    try
                    {
                        var result = ExtractResult(response);
                        pending.Tcs.TrySetResult(result!);
                    }
                    catch (Exception ex)
                    {
                        pending.Tcs.TrySetException(ex);
                    }
                }
            }
        }
        catch (Exception)
        {
            // Logger.LogError(e, "ReaderSession[{SessionId}] have error on processing server messages", SessionId);
        }
        /*
        finally
        {
            // NOTE
        }
        */
    }

    private static ulong? ExtractReqId(SessionResponse response) =>
        response.ResponseCase switch
        {
            SessionResponse.ResponseOneofCase.AcquireSemaphoreResult => response.AcquireSemaphoreResult.ReqId,
            SessionResponse.ResponseOneofCase.ReleaseSemaphoreResult => response.ReleaseSemaphoreResult.ReqId,
            SessionResponse.ResponseOneofCase.DescribeSemaphoreResult => response.DescribeSemaphoreResult.ReqId,
            SessionResponse.ResponseOneofCase.CreateSemaphoreResult => response.CreateSemaphoreResult.ReqId,
            SessionResponse.ResponseOneofCase.UpdateSemaphoreResult => response.UpdateSemaphoreResult.ReqId,
            SessionResponse.ResponseOneofCase.DeleteSemaphoreResult => response.DeleteSemaphoreResult.ReqId,
            _ => null
        };

    // возвращать пару response и код и снаружи 


    private static PendingResult? ExtractResult(SessionResponse response)
    {
        switch (response.ResponseCase)
        {
            case SessionResponse.ResponseOneofCase.AcquireSemaphoreResult:
                var acquireResult = response.AcquireSemaphoreResult;
                if (acquireResult.Status != StatusIds.Types.StatusCode.Success)
                {
                    throw new Exception(acquireResult.Status + " " + acquireResult.Issues);
                }

                return new PendingResult(response, SessionResponse.ResponseOneofCase.AcquireSemaphoreResult);

            case SessionResponse.ResponseOneofCase.ReleaseSemaphoreResult:
                var releaseResult = response.ReleaseSemaphoreResult;
                if (releaseResult.Status != StatusIds.Types.StatusCode.Success)
                {
                    throw new Exception(releaseResult.Status + " " + releaseResult.Issues);
                }

                return new PendingResult(response, SessionResponse.ResponseOneofCase.ReleaseSemaphoreResult);

            case SessionResponse.ResponseOneofCase.DescribeSemaphoreResult:
                var describeResult = response.DescribeSemaphoreResult;
                if (describeResult.Status != StatusIds.Types.StatusCode.Success)
                {
                    throw new Exception(describeResult.Status + " " + describeResult.Issues);
                }

                return new PendingResult(response, SessionResponse.ResponseOneofCase.DescribeSemaphoreResult);

            case SessionResponse.ResponseOneofCase.CreateSemaphoreResult:
                var createResult = response.CreateSemaphoreResult;
                if (createResult.Status != StatusIds.Types.StatusCode.Success)
                {
                    throw new Exception(createResult.Status + " " + createResult.Issues);
                }

                return new PendingResult(response, SessionResponse.ResponseOneofCase.CreateSemaphoreResult);

            case SessionResponse.ResponseOneofCase.UpdateSemaphoreResult:
                var updateResult = response.UpdateSemaphoreResult;
                if (updateResult.Status != StatusIds.Types.StatusCode.Success)
                {
                    throw new Exception(updateResult.Status + " " + updateResult.Issues);
                }

                return new PendingResult(response, SessionResponse.ResponseOneofCase.UpdateSemaphoreResult);

            case SessionResponse.ResponseOneofCase.DeleteSemaphoreResult:
                var deleteResult = response.DeleteSemaphoreResult;
                if (deleteResult.Status != StatusIds.Types.StatusCode.Success)
                {
                    throw new Exception(deleteResult.Status + " " + deleteResult.Issues);
                }

                return new PendingResult(response, SessionResponse.ResponseOneofCase.DeleteSemaphoreResult);

            default:
                return null;
        }
    }


    private async Task HandlePing(SessionResponse response)
    {
        var opaque = response.Ping.Opaque;
        var pongRequest = new SessionRequest
        {
            Pong = new SessionRequest.Types.PingPong
            {
                Opaque = opaque
            }
        };
        await _stream!.Write(pongRequest);
    }

    private async Task HandleFailure(SessionResponse response)
    {
        var failure = response.Failure;
        if ((failure.Status == StatusIds.Types.StatusCode.BadSession) |
            (failure.Status == StatusIds.Types.StatusCode.SessionExpired))
        {
            // If session is expired or not accessible, reset session ID to create a new session on reconnect
            //var expiredId = _sessionId;
            // _sessionId = 0;
            //_watchedSemaphores.Clear();

            // // Emit sessionExpired event to notify user
        }

        await _stream!.RequestStreamComplete();
    }

    private void HandleSessionStarted() //SessionResponse response
    {
        //_sessionId = response.SessionStarted.SessionId;
        // Resolve the sessionStarted promise
        _sessionStartedTcs?.TrySetResult();
        _sessionStartedTcs = null;

        // Resolve the first session started promise (only once)
        _firstSessionStartedTcs.TrySetResult();
    }

    private void HandleSessionStopped() //SessionResponse response
    {
        //_sessionId = response.SessionStopped.SessionId; // в логи записываем
        _sessionStoppedTcs?.TrySetResult();
        _sessionStoppedTcs = null;
    }

    /*
    private void HandleSemaphoreChanged(SessionResponse.Types.DescribeSemaphoreChanged change)
    {
        if (_watchedSemaphores.TryRemove(change.ReqId, out var name))
        {

            SemaphoreChanged?.Invoke(new SemaphoreChangedEvent
            {
                Name = name,
                DataChanged = change.DataChanged,
                OwnersChanged = change.OwnersChanged
            });
        }
    }
    */

    private async Task SendStartSession()
    {
        if (_stream == null)
            return;

        _sessionStartedTcs =
            new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var reqId = _reqIdCounter;
        var node = "";
        // var timeout = new TimeSpan(5);
        var key = ByteString.Empty;
        var initRequestStart = new SessionRequest
        {
            SessionStart =
            {
                SessionId = reqId,
                Path = node,
                TimeoutMillis = 5,
                ProtectionKey = key,
                SeqNo = _seqNo++
            }
        };

        await _stream.Write(initRequestStart);
        await _sessionStartedTcs.Task;
    }

    private async Task<PendingResult> SendRequest(ulong reqId, SessionRequest request,
        CancellationToken cancellationToken = default)
    {
        var tcs = new TaskCompletionSource<PendingResult>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        _pendingRequests[reqId] = new PendingRequest<PendingResult>(tcs);

        try
        {
            await _stream!.Write(request);
        }
        catch (Exception e)
        {
            _pendingRequests.TryRemove(reqId, out _);
            tcs.TrySetException(e);
            throw;
        }

        await using (cancellationToken.Register(() =>
                     {
                         if (_pendingRequests.TryRemove(reqId, out var pending))
                         {
                             pending.Tcs.TrySetCanceled(cancellationToken);
                         }
                     }))
        {
            // timeout + cancellation?
            return await tcs.Task;
        }
    }

    /**
     * Gets the next request ID
     */
    private ulong GetNextReqId()
        => Interlocked.Increment(ref _reqIdCounter);


    public async Task CreateSemaphore(string name, ulong limit, byte[]? data)
    {
        var reqId = GetNextReqId();
        var createSemaphore = new SessionRequest
        {
            CreateSemaphore =
            {
                Name = name,
                Limit = limit,
                Data = data == null ? ByteString.Empty : ByteString.CopyFrom(data),
                ReqId = reqId
            }
        };
        try
        {
            var task = SendRequest(reqId, createSemaphore);
            await task;
        }
        catch (Exception)
        {
            throw new YdbException("Create semaphore failed");
        }
    }


    public async Task UpdateSemaphore(string name, byte[]? data)
    {
        var reqId = GetNextReqId();
        var updateSemaphore = new SessionRequest
        {
            UpdateSemaphore =
            {
                Name = name,
                Data = data == null ? ByteString.Empty : ByteString.CopyFrom(data),
                ReqId = reqId
            }
        };
        try
        {
            var task = SendRequest(reqId, updateSemaphore);
            await task;
        }
        catch (Exception)
        {
            throw new YdbException("Update semaphore failed");
        }
    }


    public async Task DeleteSemaphore(string name, bool force)
    {
        var reqId = GetNextReqId();
        var deleteSemaphore = new SessionRequest
        {
            DeleteSemaphore =
            {
                Name = name,
                Force = force,
                ReqId = reqId
            }
        };
        try
        {
            var task = SendRequest(reqId, deleteSemaphore);
            await task;
        }
        catch (Exception)
        {
            throw new YdbException("Delete semaphore failed");
        }
    }


    public async Task<SessionResponse.Types.DescribeSemaphoreResult> DescribeSemaphore(string name,
        DescribeSemaphoreMode mode)
    {
        var reqId = GetNextReqId();
        var request = new SessionRequest
        {
            DescribeSemaphore =
            {
                Name = name,
                IncludeOwners = DescribeSemaphoreModeUtils.IncludeOwners(mode),
                IncludeWaiters = DescribeSemaphoreModeUtils.IncludeWaiters(mode),
                WatchData = false,
                WatchOwners = false,
                ReqId = reqId
            }
        };

        try
        {
            var task = await SendRequest(reqId, request);
            return task.Request.DescribeSemaphoreResult;
        }
        catch (Exception)
        {
            throw new YdbException("Delete semaphore failed");
        }
    }

    /*

    public async void WatchSemaphore(string name, DescribeSemaphoreMode describeMode, WatchSemaphoreMode watchMode)
    {
    }


    public async void AcquireSemaphore(string name, long count, byte[] data,
        TimeSpan timeout)
    {
    }


    public async void AcquireEphemeralSemaphore(String name, bool exclusive,
        byte[] data, TimeSpan timeout)
    {
    }

    public async void ReleaseSemaphore(String name)
    {
    }
    */
}
