using System.Threading.Channels;
using Ydb.Sdk.GrpcWrappers.Topic.Writer.Init;
using Ydb.Sdk.GrpcWrappers.Topic.Writer.Write;
using Ydb.Sdk.Services.Topic.Models.Writer;
using Ydb.Sdk.Utils;
using Codec = Ydb.Sdk.GrpcWrappers.Topic.Codecs.Codec;
using StreamWriter = Ydb.Sdk.GrpcWrappers.Topic.StreamWriter;
using PublicMessage = Ydb.Sdk.Services.Topic.Models.Writer.Message;
using PublicCodec = Ydb.Sdk.Services.Topic.Models.Codec;
using Message = Ydb.Sdk.GrpcWrappers.Topic.Writer.Write.Message;

namespace Ydb.Sdk.Services.Topic.Internal;

internal class WriterReconnector: IDisposable, IAsyncDisposable
{
    private const int CheckBatchesInterval = 10_000;

    // ReSharper disable InconsistentNaming
    private readonly Driver driver;
    private readonly Encoders encoders;
    private readonly Queue<Message> allMessages = new();
    private readonly Channel<Message> newMessages = Channel.CreateUnbounded<Message>();
    private readonly Channel<Message> messagesToEncodeQueue = Channel.CreateUnbounded<Message>();
    private readonly Channel<Message> messagesToSendQueue = Channel.CreateUnbounded<Message>();
    private readonly Queue<TaskCompletionSource<WriteResult>> writeResults = new();
    private readonly List<Task> backgroundTasks;
    private readonly WriterConfig config;
    
    private InitRequest initRequest;
    private InitInfo? initInfo;
    private PublicCodec? codec;
    private long lastKnownSequenceNumber;
    private PublicCodec? lastSelectedCodec;
    private int batchNumber;

    public WriterReconnector(Driver driver, WriterConfig config)
    {
        this.driver = driver;
        backgroundTasks = new List<Task>
        {
            Task.Run(ConnectionLoop),
            Task.Run(EncodeLoop)
        };
        encoders = new Encoders();
        foreach (var encoder in config.Encoders)
        {
            var internalCodec = EnumConverter.Convert<PublicCodec, Codec>(encoder.Key);
            encoders.Add(internalCodec, encoder.Value);
        }
    }

    public async Task<InitInfo> WaitInit(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            if (initInfo != null)
                return initInfo;
        }
    }

    public async Task<List<Task<WriteResult>>> Write(List<PublicMessage> messages)
    {
        var internalMessages = PrepareMessages(messages);
        var taskSources = Enumerable
            .Range(0, messages.Count)
            .Select(_ => new TaskCompletionSource<WriteResult>())
            .ToList();
        taskSources.ForEach(source => writeResults.Enqueue(source));

        if (codec == PublicCodec.Raw)
        {
            await EnqueueSendMessages(internalMessages);
        }
        else
        {
            foreach (var message in internalMessages)
            {
                await messagesToEncodeQueue.Writer.WriteAsync(message);
            }
        }

        return taskSources.Select(s => s.Task).ToList();
    }

    public async Task Flush()
    {
        await Task.WhenAll(writeResults.Select(source => source.Task));
    }

    public void Dispose()
    {
        
    }

    public async ValueTask DisposeAsync()
    {
        
    }

    private async Task ConnectionLoop()
    {
        while (true)
        {
            try
            {
                var streamWriter = await StreamWriter.Init(driver, initRequest);

                if (initInfo == null)
                {
                    lastKnownSequenceNumber = streamWriter.LastSequenceNumber;
                    initInfo = new InitInfo
                    {
                        LastSequenceNumber = streamWriter.LastSequenceNumber,
                        SupportedCodecs = streamWriter.SupportedCodecs.ToPublic().ToList()
                    };
                }
                var sendLoopTask = Task.Run(async () => await SendLoop(streamWriter));
                var receiveLoopTask = Task.Run(async () => await ReadLoop(streamWriter));
                await Task.WhenAny(sendLoopTask, receiveLoopTask);
            }
            catch (Exception e)
            {
            
            }
        }
    }

    private async Task SendLoop(StreamWriter streamWriter)
    {
        var messages = allMessages.ToList();
        var lastSequenceNumber = 0L;
        
        foreach (var message in messages)
        {
            //await streamWriter.Write();
            lastSequenceNumber = message.SequenceNumber;
        }

        while (true)
        {
            var message = await newMessages.Reader.ReadAsync();
            if (message.SequenceNumber > lastSequenceNumber)
            {
                //TODO
                // await streamWriter.Write(new List<WriteRequest> {new WriteRequest {me}});
            }
        }
    }

    private async Task ReadLoop(StreamWriter streamWriter)
    {
        while (true)
        {
            var response = await streamWriter.Receive();
            response.Result.Acks.ForEach(HandleAck);
        }
    }

    private async Task EncodeLoop()
    {
        while (true)
        {
            var messages = await messagesToEncodeQueue.Reader.ReadAllAsync().ToListAsync();
            await EncodeMessages(codec.Value, messages);
            foreach (var message in messages)
            {
                await messagesToSendQueue.Writer.WriteAsync(message);
            }
        }
    }

    private void HandleAck(WriteAck ack)
    {
        var currentMessage = allMessages.Dequeue();
        var taskSource = writeResults.Dequeue();
        if (currentMessage.SequenceNumber != ack.SequenceNumber)
            /*TODO*/;
        var writeStatus = ack.WriteStatus;
        switch (writeStatus.Type)
        {
            case WriteStatusType.Skipped:
            case WriteStatusType.Written:
                taskSource.SetResult(WriteResult.FromWrapper(writeStatus));
                break;
            default:
                throw new NotImplementedException();
        }
    }

    private async Task EnqueueSendMessages(List<Message> messages)
    {
        foreach (var message in messages)
        {
            allMessages.Enqueue(message);
            await newMessages.Writer.WriteAsync(message);
        }
    }

    private async Task EncodeMessages(PublicCodec publicCodec, List<Message> messages)
    {
        if (publicCodec == PublicCodec.Raw)
            return;

        var internalCodec = EnumConverter.Convert<PublicCodec, Codec>(publicCodec);
        var tasks = messages.Select(m => Task.Run(() => encoders.Encode(internalCodec, m.Data)));
        var encodedContents = await Task.WhenAll(tasks);
        for (var i = 0; i < encodedContents.Length; i++)
        {
            messages[i].Codec = internalCodec;
            messages[i].Data = encodedContents[i];
        }
    }

    private List<Message> PrepareMessages(List<PublicMessage> publicMessages)
    {
        var now = config.AutoSetCreatedAt ? DateTime.Now : default;

        var result = new List<Message>();
        foreach (var internalMessage in publicMessages.Select(message => message.ToWrapper()))
        {
            if (config.AutoSetSequenceNumber)
            {
                if (internalMessage.SequenceNumber >= 0) ;
                //TODO throw
                lastKnownSequenceNumber++;
                internalMessage.SequenceNumber = lastKnownSequenceNumber;
            }
            else
            {
                if (internalMessage.SequenceNumber < 0) ;
                //TODO throw
                else if (internalMessage.SequenceNumber <= lastKnownSequenceNumber) ;
                //TODO throw
                else
                    lastKnownSequenceNumber = internalMessage.SequenceNumber;
            }

            if (config.AutoSetCreatedAt)
            {
                if (internalMessage.CreatedAt != default) ;
                //TODO throw
                else
                    internalMessage.CreatedAt = now;
            }
            result.Add(internalMessage);
        }

        return result;
    }

    private InitRequest CreateInitRequest()
    {
        return new InitRequest
        {

        };
    }
}
