using System.Collections.Concurrent;
using Google.Protobuf;
using Ydb.Coordination;
using Ydb.Coordination.V1;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.RetryPolicy;
using Ydb.Sdk.Coordinator.Description;
using Ydb.Sdk.Coordinator.Settings;

namespace Ydb.Sdk.Coordinator.Impl;

public class CoordinationSession
{
    private readonly IDriver _driver;
    private readonly CoordinationNodeSettings _settings;
    private readonly YdbRetryPolicyExecutor _ydbRetryPolicyExecutor;


    private ulong _sessionId = 0;
    private ulong _reqIdCounter = 0;
    private ulong _seqNo = 0;


    // Tasks for waiting SessionStarted and SessionStopped
    private TaskCompletionSource? _sessionStartedTcs;
    private TaskCompletionSource? _sessionStoppedTcs;

    // Bidirectional stream handler
    private IBidirectionalStream<SessionRequest, SessionResponse>? _stream;
    private readonly ConcurrentDictionary<ulong, PendingRequest<bool, SessionRequest>> _pendingRequests = new();
    private readonly List<SessionRequest> _fireAndForgetRequests = new();

    // Reconnection state
    private bool _closed = false;

    // Map of _reqIdCounter to semaphore name for watch tracking
    private readonly ConcurrentDictionary<ulong, string> _watchedSemaphores =
        new();


    private readonly TaskCompletionSource _firstSessionStartedTcs =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    // EVENTS
    //public event Action<SessionExpiredEvent>? SessionExpired;
    //public event Action<SemaphoreChangedEvent>? SemaphoreChanged;    

    private readonly CancellationTokenSource _disposeCts = new();

    public CoordinationSession(IDriver driver, CoordinationNodeSettings settings, IRetryPolicy retryPolicy)
    {
        _ydbRetryPolicyExecutor = new YdbRetryPolicyExecutor(retryPolicy);
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
                        HandleSessionStarted(response);
                        break;
                    case SessionResponse.ResponseOneofCase.SessionStopped:
                        HandleSessionStopped(response);
                        break;
                    case SessionResponse.ResponseOneofCase.AcquireSemaphorePending:
                        break;
                    case SessionResponse.ResponseOneofCase.DescribeSemaphoreChanged:
                        break;
                    case SessionResponse.ResponseOneofCase.AcquireSemaphoreResult:
                        break;
                }
                /*
                 * None,
    Ping,
    Pong,
    Failure,
    SessionStarted,
    SessionStopped,
    Unsupported6,
    Unsupported7,
    AcquireSemaphorePending,
    AcquireSemaphoreResult,
    ReleaseSemaphoreResult,
    DescribeSemaphoreResult,
    DescribeSemaphoreChanged,
    CreateSemaphoreResult,
    UpdateSemaphoreResult,
    DeleteSemaphoreResult,
    Unsupported16,
    Unsupported17,
    Unsupported18,
                 */

                //var reqId = _extractReqId(response); //_extractReqId(response);
                ulong? reqId = 1;
                if (reqId.HasValue &&
                    _pendingRequests.TryRemove(reqId.Value, out var pending))
                {
                    try
                    {
                        //var result = _extractResult(response);
                        //pending.Tcs.TrySetResult(result);
                    }
                    catch (Exception ex)
                    {
                        pending.Tcs.TrySetException(ex);
                    }
                }
            }
        }
        catch (Exception e)
        {
            // Logger.LogError(e, "ReaderSession[{SessionId}] have error on processing server messages", SessionId);
        }
        finally
        {
            // NOTE
        }
    }

    private async Task HandlePing(SessionResponse response)
    {
        var opaque = response.Ping.Opaque;
        var pongRequest = new SessionRequest()
        {
            Pong = new SessionRequest.Types.PingPong()
            {
                Opaque = opaque
            }
        };
        await _stream!.Write(pongRequest);
    }

    private async Task HandleFailure(SessionResponse response)
    {
        var failure = response.Failure;
        if (failure.Status == StatusIds.Types.StatusCode.BadSession |
            failure.Status == StatusIds.Types.StatusCode.SessionExpired)
        {
            // If session is expired or not accessible, reset session ID to create a new session on reconnect
            //var expiredId = _sessionId;
            _sessionId = 0;
            _watchedSemaphores.Clear();

            // // Emit sessionExpired event to notify user
        }

        await _stream!.RequestStreamComplete();
    }

    private void HandleSessionStarted(SessionResponse response)
    {
        _sessionId = response.SessionStarted.SessionId;
        // Resolve the sessionStarted promise
        _sessionStartedTcs?.TrySetResult();
        _sessionStartedTcs = null;

        // Resolve the first session started promise (only once)
        _firstSessionStartedTcs.TrySetResult();
    }

    private void HandleSessionStopped(SessionResponse response)
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
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        ulong reqId = 1;
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
                ProtectionKey = key
            }
        };

        await _stream.Write(initRequestStart);
        await _sessionStartedTcs.Task;
    }

    public async Task CreateSemaphore(string name, ulong limit, byte[]? data)
    {
        ulong reqId = 1;
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
            var task = _stream!.Write(createSemaphore);
            await task;
            if (task.IsFaulted)
            {
                throw new YdbException("Create semaphore failed");
            }

            if (task.IsCanceled)
            {
                throw new YdbException("Create semaphore canceled");
            }
        }
        catch (YdbException e)
        {
            throw new YdbException(e.Message);
        }
        catch (Exception)
        {
            throw new YdbException("Create semaphore failed");
        }
    }


    public async Task UpdateSemaphore(string name, byte[]? data)
    {
        ulong reqId = 1;
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
            var task = _stream!.Write(updateSemaphore);
            await task;
            if (task.IsFaulted)
            {
                throw new YdbException("Update semaphore failed");
            }

            if (task.IsCanceled)
            {
                throw new YdbException("Update semaphore canceled");
            }
        }
        catch (YdbException e)
        {
            throw new YdbException(e.Message);
        }
        catch (Exception)
        {
            throw new YdbException("Update semaphore failed");
        }
    }


    public async Task DeleteSemaphore(string name, bool force)
    {
        ulong reqId = 1;
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
            var task = _stream!.Write(deleteSemaphore);
            await task;
            if (task.IsFaulted)
            {
                throw new YdbException("Delete semaphore failed");
            }

            if (task.IsCanceled)
            {
                throw new YdbException("Delete semaphore canceled");
            }
        }
        catch (YdbException e)
        {
            throw new YdbException(e.Message);
        }
        catch (Exception)
        {
            throw new YdbException("Delete semaphore failed");
        }
    }

    /*
    public async void DescribeSemaphore(string name, DescribeSemaphoreMode mode)
    {
    }


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
