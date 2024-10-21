namespace Ydb.Sdk.Services.Topic;

public interface IProducer<TValue>
{
    public Task<SendResult> SendAsync(TValue data);

    public Task<SendResult> SendAsync(Message<TValue> message);
}
