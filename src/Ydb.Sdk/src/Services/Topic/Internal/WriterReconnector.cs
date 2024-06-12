using System.Threading.Channels;
using Ydb.Sdk.GrpcWrappers.Topic.Writer.Init;
using Ydb.Sdk.GrpcWrappers.Topic.Writer.Write;
using Ydb.Sdk.Services.Topic.Models;
using Ydb.Sdk.Services.Topic.Models.Writer;
using StreamWriter = Ydb.Sdk.GrpcWrappers.Topic.StreamWriter;
using PublicMessage = Ydb.Sdk.Services.Topic.Models.Writer.Message;
using PublicCodec = Ydb.Topic.Codec;
using Message = Ydb.Sdk.GrpcWrappers.Topic.Writer.Write.Message;

namespace Ydb.Sdk.Services.Topic.Internal;

internal class WriterReconnector: IDisposable, IAsyncDisposable
{
    // ReSharper disable InconsistentNaming
    private readonly Driver driver;
    private readonly Encoders encoders;
    private readonly Queue<Message> allMessages = new();
    private readonly Channel<Message> newMessages = Channel.CreateUnbounded<Message>();
    private readonly Channel<Message> messagesToEncodeQueue = Channel.CreateUnbounded<Message>();
    private readonly Channel<Message> messagesToSendQueue = Channel.CreateUnbounded<Message>();
    private readonly Queue<TaskCompletionSource<WriteResult>> writeResults = new();
    private readonly List<Task> backgroundTasks;
    
    private InitRequest initRequest;
    private InitInfo? initInfo;
    private PublicCodec codec;

    public WriterReconnector(Driver driver, WriterConfig config)
    {
        this.driver = driver;
        backgroundTasks = new List<Task>
        {
            Task.Run(ConnectionLoop),
            Task.Run(EncodeLoop)
        };
    }

    public InitInfo WaitInit()
    {
        while (true)
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
                await using var streamWriter = await StreamWriter.Init(driver, initRequest);
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
            var selectedCodec = await ChooseCodec(messages);
            messages = await EncodeMessages(selectedCodec, messages);
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

    private async Task<List<Message>> EncodeMessages(PublicCodec codec, List<Message> messages)
    {
        throw new NotImplementedException();
    }

    private async Task<PublicCodec> ChooseCodec(List<Message> messages)
    {
        throw new NotImplementedException();
    }

    private List<Message> PrepareMessages(List<PublicMessage> publicMessages)
    {
        // auto sequenceNumber?
        throw new NotImplementedException();
    }

    private Codec ChooseCodecByCompressionPower()
    {
        // На ограниченной выборке сообщений берется алгоритм сжатия с наименьшим суммарным размером контента
        throw new NotImplementedException();
    }

    private InitRequest CreateInitRequest()
    {
        return new InitRequest
        {

        };
    }
}
