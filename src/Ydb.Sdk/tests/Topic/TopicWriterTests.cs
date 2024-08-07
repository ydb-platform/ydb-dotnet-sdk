using System.IO.Compression;
using System.Text;
using Google.Protobuf.WellKnownTypes;
using Xunit;
using Ydb.Sdk.Services.Topic;
using Ydb.Sdk.Services.Topic.Models.Writer;
using Ydb.Sdk.Tests.Fixture;
using Ydb.Topic;
using Codec = Ydb.Sdk.Services.Topic.Models.Codec;
using SupportedCodecs = Ydb.Sdk.GrpcWrappers.Topic.Codecs.SupportedCodecs;

namespace Ydb.Sdk.Tests.Topic;

[Trait("Category", "Integration")]
public class TopicWriterTests : IClassFixture<TopicClientFixture>
{
    private const string Consumer = "cons";

    private readonly string _topic = Guid.NewGuid().ToString();
    private readonly TopicClient _topicClient;
    private readonly Driver _driver;

    public TopicWriterTests(TopicClientFixture fixture)
    {
        _topicClient = fixture.TopicClient;
        var driverConfig = new DriverConfig(
            endpoint: "grpc://localhost:2136",
            database: "/local"
        );
        _driver = new Driver(driverConfig, Utils.GetLoggerFactory());
    }

    [Fact]
    public async Task TestWriteMessage()
    {
        await _driver.Initialize();
        await CreateTopic();

        var content = Encoding.UTF8.GetBytes("content");
        var writer = _topicClient.StartWriter(_topic);
        await writer.WaitInit();
        var message = new Message
        {
            Data = content
        };
        var results = await writer.Write(new List<Message> {message});
        Assert.All(results, result => Assert.Equal(WriteResultStatus.Written, result.Status));

        var readResult = await Read();
        Assert.Equal((int) Codec.Gzip, readResult.Codec);

        var receivedMessage = readResult.MessageData.First();
        Assert.Equal(1, receivedMessage.SeqNo);

        Assert.Equal(Gzip(content), receivedMessage.Data.ToArray());

        await DropTopic();

        byte[] Gzip(byte[] data)
        {
            using var gzippedDataStream = new MemoryStream();
            using var gzipStream = new GZipStream(gzippedDataStream, CompressionMode.Compress);
            gzipStream.Write(data);

            return gzippedDataStream.ToArray();
        }
    }

    private async Task CreateTopic()
    {
        var supportedCodecs = new SupportedCodecs(new[]
            {GrpcWrappers.Topic.Codecs.Codec.Raw, GrpcWrappers.Topic.Codecs.Codec.Gzip}).ToProto();
        var request = new CreateTopicRequest
        {
            Path = _topic,
            SupportedCodecs = supportedCodecs,
            Consumers =
            {
                new Consumer
                {
                    SupportedCodecs = supportedCodecs,
                    ReadFrom = new Timestamp(),
                    Name = Consumer
                }
            },
        };
        var response = await _driver.UnaryCall(Ydb.Topic.V1.TopicService.CreateTopicMethod, request,
            new GrpcRequestSettings());
        Assert.Equal(StatusIds.Types.StatusCode.Success, response.Operation.Status);
    }

    private async Task DropTopic()
    {
        var request = new DropTopicRequest
        {
            Path = _topic
        };
        await _driver.UnaryCall(Ydb.Topic.V1.TopicService.DropTopicMethod, request, new GrpcRequestSettings());
    }

    private async Task<StreamReadMessage.Types.ReadResponse.Types.Batch> Read()
    {
        var initStream =
            _driver.DuplexStreamCall(Ydb.Topic.V1.TopicService.StreamReadMethod, new GrpcRequestSettings());
        await initStream.Write(new StreamReadMessage.Types.FromClient
        {
            InitRequest = new StreamReadMessage.Types.InitRequest
            {
                Consumer = Consumer, ReaderName = "reader", TopicsReadSettings =
                {
                    new StreamReadMessage.Types.InitRequest.Types.TopicReadSettings
                    {
                        ReadFrom = new Timestamp(),
                        Path = _topic
                    }
                }
            }
        });
        await initStream.MoveNextAsync();
        await initStream.Write(new StreamReadMessage.Types.FromClient
        {
            ReadRequest = new StreamReadMessage.Types.ReadRequest
            {
                BytesSize = 100
            }
        });
        await initStream.MoveNextAsync();
        var startRequest = initStream.Current.StartPartitionSessionRequest;
        await initStream.Write(new StreamReadMessage.Types.FromClient
        {
            StartPartitionSessionResponse = new StreamReadMessage.Types.StartPartitionSessionResponse
            {
                CommitOffset = startRequest.CommittedOffset,
                PartitionSessionId = startRequest.PartitionSession.PartitionSessionId,
                ReadOffset = 0
            }
        });
        await initStream.MoveNextAsync();
        var readResponse = initStream.Current.ReadResponse;
        return readResponse.PartitionData.First().Batches.First();
    }
}
