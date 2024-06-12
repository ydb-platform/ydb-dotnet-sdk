using System.Threading.Channels;
using Ydb.Sdk.GrpcWrappers.Topic.Reader;
using Ydb.Sdk.GrpcWrappers.Topic.Reader.Batch;
using Ydb.Sdk.Services.Topic;
using Ydb.Sdk.Services.Topic.Models.Reader;
using static Ydb.Topic.StreamReadMessage.Types;
using Batch = Ydb.Sdk.GrpcWrappers.Topic.Reader.Batch.Batch;
using CommitOffsetResponse = Ydb.Sdk.GrpcWrappers.Topic.Reader.CommitOffset.CommitOffsetResponse;
using InitRequest = Ydb.Sdk.GrpcWrappers.Topic.Reader.Init.InitRequest;
using InitResponse = Ydb.Sdk.GrpcWrappers.Topic.Reader.Init.InitResponse;
using OffsetsRange = Ydb.Topic.OffsetsRange;
using ReadResponse = Ydb.Sdk.GrpcWrappers.Topic.Reader.Read.ReadResponse;
using StartPartitionSessionRequest = Ydb.Sdk.GrpcWrappers.Topic.Reader.StartPartitionSession.StartPartitionSessionRequest;
using StopPartitionSessionRequest = Ydb.Sdk.GrpcWrappers.Topic.Reader.StopPartitionSession.StopPartitionSessionRequest;
using PartitionSession = Ydb.Sdk.GrpcWrappers.Topic.Reader.StartPartitionSession.PartitionSession;
using ReadRequest = Ydb.Sdk.GrpcWrappers.Topic.Reader.Read.ReadRequest;

namespace Ydb.Sdk.GrpcWrappers.Topic;

internal class StreamReader: IDisposable, IAsyncDisposable
{
    // ReSharper disable InconsistentNaming
    private bool isInitialized;
    private string sessionId;
    private long readerReconnectorId;
    private long bufferFreeSpaceInBytes;

    private readonly ReadMessagesResponseStream responseStream;
    private readonly Driver.BidirectionalStream<FromClient, FromServer> stream;
    private readonly Channel<Batch> batchesToDecodeQueue = Channel.CreateUnbounded<Batch>();
    private readonly Channel<Batch> batchesQueue = Channel.CreateUnbounded<Batch>();
    private readonly Dictionary<long, PartitionSession> partitionSessions = new();
    private readonly List<Task> tasks = new();

    private StreamReader(
        long readerReconnectorId,
        ReadMessagesResponseStream responseStream,
        Driver.BidirectionalStream<FromClient, FromServer> stream)
    {
        this.readerReconnectorId = readerReconnectorId;
        this.responseStream = responseStream;
        this.stream = stream;
    }

    public static async Task<StreamReader> Create(
        int readerReconnectorId,
        Driver driver,
        ReaderConfig config)
    {
        var reader = driver.DuplexStreamCall(Ydb.Topic.V1.TopicService.StreamReadMethod, new GrpcRequestSettings());

        var responseStream = new ReadMessagesResponseStream(default/*reader*/);
        var streamReader = new StreamReader(readerReconnectorId, responseStream, reader);

        await streamReader.Init();
        return streamReader;
    }

    public async Task WaitMessage()
    {
        while (true)
        {
            if (batchesQueue.Reader.CanPeek)
                return;
        }
    }

    public async Task<MessageData?> ReceiveMessage()
    {
        batchesQueue.Reader.TryPeek(out var batch);
        var message = batch?.MessageData.FirstOrDefault();
        if (message is null)
        {
            await PopBatch();
            return null;
        }

        batch!.MessageData.RemoveAt(0);
        return message;
    }

    public async Task<Batch> ReceiveBatch()
    {
        return await PopBatch();
    }

    public async Task Commit(CommitRange commitRange)
    {
        var commitRequest = new CommitOffsetRequest
        {
            CommitOffsets =
            {
                new CommitOffsetRequest.Types.PartitionCommitOffset
                {
                    PartitionSessionId = commitRange.PartitionSession.Id,
                    Offsets =
                    {
                        new OffsetsRange
                        {
                            Start = commitRange.CommitOffsetStart,
                            End = commitRange.CommitOffsetEnd
                        }
                    }
                }
            }
        };

        var request = new FromClient {CommitOffsetRequest = commitRequest};
        await stream.Write(request);
    }

    private async Task Init()
    {
        if (isInitialized)
            throw new InvalidOperationException("Reader is already started");
        isInitialized = true;

        var initRequest = CreateInitRequest();
        var request = new FromClient
        {
            InitRequest = initRequest.ToProto()
        };
        await stream.Write(request);

        if (!await responseStream.Next())
            throw new Exception("No init response received");
        var initResponse = (InitResponse) responseStream.Response;
        sessionId = initResponse.Result.SessionId;

        tasks.Add(Task.Run(ReadLoop));
        tasks.Add(Task.Run(DecodeBatchesLoop));
    }

    private async Task ReadLoop()
    {
        var request = new FromClient {ReadRequest = new ReadRequest {BytesCount = bufferFreeSpaceInBytes}.ToProto()};
        await stream.Write(request);
        while (true)
        {
            if (!await responseStream.Next());
            //TODO
            var response = responseStream.Response;
            switch (response)
            {
                case ReadResponse readResponse:
                    OnReadResponse(readResponse);
                    break;
                case CommitOffsetResponse commitOffsetResponse:
                    OnCommitResponse(commitOffsetResponse);
                    break;
                case StartPartitionSessionRequest startPartitionSessionRequest:
                    OnStartPartitionSession(startPartitionSessionRequest);
                    break;
                case StopPartitionSessionRequest stopPartitionSessionRequest:
                    OnPartitionSessionStop(stopPartitionSessionRequest);
                    break;
                default:
                    throw new Exception(); //TODO
            }
        }
        
    }

    private async Task DecodeBatchesLoop()
    {
        while (true)
        {
            var batch = await batchesToDecodeQueue.Reader.ReadAsync();
            var decodedBatch = await DecodeBatch(batch);
            await batchesQueue.Writer.WriteAsync(decodedBatch);
        }
    }

    private void OnReadResponse(ReadResponse message)
    {
        ConsumeBuffer(message.Result.BytesCount);
        var batches = ConvertReadResponseToBatches(message);
        batches.ForEach(b => batchesToDecodeQueue.Writer.WriteAsync(b));
    }

    private void OnCommitResponse(CommitOffsetResponse message)
    {
        foreach (var partitionCommittedOffset in message.Result.PartitionsCommittedOffsets)
        {
            if (!partitionSessions.TryGetValue(partitionCommittedOffset.PartitionSessionId, out var session))
                continue;
            
        }
    }

    private void OnStartPartitionSession(StartPartitionSessionRequest message)
    {
        
    }

    private void OnPartitionSessionStop(StopPartitionSessionRequest message)
    {
        var data = message.Result;
        if (!partitionSessions.ContainsKey(data.PartitionSessionId))
            ;
        partitionSessions.Remove(data.PartitionSessionId);

        if (data.IsGraceful)
        {
            
        }
    }

    private List<Batch> ConvertReadResponseToBatches(ReadResponse message)
    {
        var batches = new List<Batch>();
        var batchesCount = message.Result.PartitionData.Sum(p => p.Batches.Count);
        if (batchesCount == 0)
            return batches;

        var bytesPerBatch = message.Result.BytesCount / batchesCount;
        var bytesLeft = message.Result.BytesCount % batchesCount;

        foreach (var partitionData in message.Result.PartitionData)
        {
            var partitionSession = partitionSessions[partitionData.PartitionSessionId];
            foreach (var batch in partitionData.Batches)
            {
                // var messages = new List<>();
                // foreach (var data in batch.MessageData)
                // {
                //      new Message
                //      {
                //          SequenceNumber = data.SequenceNumber,
                //          CreatedAt = data.CreatedAt,
                //          MessageGroupId = data.MessageGroupId,
                //          SessionMetadata = batch.WriteSessionMeta,
                //          Offset = data.Offset,
                //          WrittenAt = batch.WrittenAt,
                //          Data = data.Data,
                //          
                //      }
                // }
            }
        }

        return batches;
    }

    private async Task<Batch> DecodeBatch(Batch batch)
    {
        throw new NotImplementedException();
    }

    private async Task<Batch> PopBatch()
    {
        var batch = await batchesQueue.Reader.ReadAsync();
        await ReleaseBufferSpace(0/*batch.*/);
        return batch;
    }

    private void ConsumeBuffer(long bytesCountToConsume) => bufferFreeSpaceInBytes -= bytesCountToConsume;

    private async Task ReleaseBufferSpace(long bytesCountToRelease)
    {
        bufferFreeSpaceInBytes += bytesCountToRelease;
        await stream.Write(new FromClient
        {
            ReadRequest = CreateReadRequest(bytesCountToRelease).ToProto()
        });
    }

    private ReadRequest CreateReadRequest(long bytesCount) => new() {BytesCount = bytesCount};

    private InitRequest CreateInitRequest()
    {
        return new InitRequest
        {

        };
    }

    public void Dispose()
    {
        throw new NotImplementedException();
    }

    public ValueTask DisposeAsync()
    {
        throw new NotImplementedException();
    }
}
