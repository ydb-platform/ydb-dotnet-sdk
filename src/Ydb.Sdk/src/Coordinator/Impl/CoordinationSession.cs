using Google.Protobuf;
using Ydb.Coordination;
using Ydb.Coordination.V1;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Coordinator.Settings;

namespace Ydb.Sdk.Coordinator.Impl;

public class CoordinationSession
{
    private readonly IDriver _driver;
    private readonly CoordinationNodeSettings _settings;
    private IBidirectionalStream<SessionRequest, SessionResponse>? _stream;

    //private readonly string _path;
    //private readonly ulong _timeoutMillis;
    //private readonly string _description;
    //private ulong _sessionId = 0;
    //private ulong _reqIdCounter = 0;
    //private ulong _seqNo = 0;

    // Resolve functions for waiting SessionStarted and SessionStopped
    //private Func<Task> _sessionStartedResolve = null;
    //private Func<Task> _sessionStoppedResolve = null;

    public CoordinationSession(IDriver driver, CoordinationNodeSettings settings)
    {
        _driver = driver;
        _settings = settings;
        Initialize();
    }


    private async void Initialize()
    {
        try
        {
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
        ulong reqId = 1;
        var node = "";
        // var timeout = new TimeSpan(5);
        var key = ByteString.Empty;
        _stream = await _driver.BidirectionalStreamCall(CoordinationService.SessionMethod, _settings);
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

        if (!await _stream.MoveNextAsync())
        {
            // что-то делаем,если сообщения не отправилось
        }
        //прием сообщения
        // var receivedInitMessage = _stream.Current;
    }

    /*
    private async void HandleResponse(SessionResponse response)
    {
        switch (response.ResponseCase)
        {
            case SessionResponse.ResponseOneofCase.Ping:
                // Server sent ping, respond with pong
                var opaque = response.Ping.Opaque;
                var pongRequest = new SessionRequest()
                {
                    Pong = new SessionRequest.Types.PingPong()
                    {
                        Opaque = opaque
                    }
                };
                await _stream.Write(pongRequest);
                break;

            case SessionResponse.ResponseOneofCase.Failure:
                var failure = response.Failure;
                if (failure.Status == StatusIds.Types.StatusCode.BadSession |
                    failure.Status == StatusIds.Types.StatusCode.SessionExpired)
                {
                    // If session is expired or not accessible, reset session ID to create a new session on reconnect
                }

                //await stream.RequestStreamComplete();
                break;
            case SessionResponse.ResponseOneofCase.SessionStarted:
                _sessionId = response.SessionStarted.SessionId;
                break;
            case SessionResponse.ResponseOneofCase.SessionStopped:
                _sessionId = response.SessionStopped.SessionId;
                break;
            case SessionResponse.ResponseOneofCase.AcquireSemaphorePending:
                break;
            case SessionResponse.ResponseOneofCase.DescribeSemaphoreChanged:
                break;
            default:
                break;
        }
    }
    */

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
