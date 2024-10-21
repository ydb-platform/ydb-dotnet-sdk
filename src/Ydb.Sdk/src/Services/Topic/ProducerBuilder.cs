using Ydb.Topic;
using Ydb.Topic.V1;

namespace Ydb.Sdk.Services.Topic;

public class ProducerBuilder<TValue>
{
    private readonly ProducerConfig _config;

    public ProducerBuilder(ProducerConfig config)
    {
        _config = config;
    }

    public ISerializer<TValue>? Serializer { get; set; }

    public async Task<IProducer<TValue>> Build()
    {
        var stream = _config.Driver.BidirectionalStreamCall(TopicService.StreamWriteMethod,
            GrpcRequestSettings.DefaultInstance);

        var initRequest = new StreamWriteMessage.Types.InitRequest { Path = _config.TopicPath };
        if (_config.ProducerId != null)
        {
            initRequest.ProducerId = _config.ProducerId;
        }

        if (_config.MessageGroupId != null)
        {
            initRequest.MessageGroupId = _config.MessageGroupId;
        }

        await stream.Write(new StreamWriteMessage.Types.FromClient { InitRequest = initRequest });
        if (!await stream.MoveNextAsync())
        {
            throw new YdbProducerException("Write stream is closed by YDB server");
        }

        var receivedInitMessage = stream.Current;

        Status.FromProto(receivedInitMessage.Status, receivedInitMessage.Issues).EnsureSuccess();

        var initResponse = receivedInitMessage.InitResponse;

        if (!initResponse.SupportedCodecs.Codecs.Contains((int)_config.Codec))
        {
            throw new YdbProducerException($"Topic is not supported codec: {_config.Codec}");
        }

        return new Producer<TValue>(
            _config, initResponse, stream,
            Serializer ?? (ISerializer<TValue>)(
                Serializers.DefaultSerializers.TryGetValue(typeof(TValue), out var serializer)
                    ? serializer
                    : throw new YdbProducerException("The serializer is not set")
            )
        );
    }
}
