using Ydb.Sdk.Services.Topic.Writer;

namespace Ydb.Sdk.Services.Topic;

public interface IWriter<TValue> : IDisposable
{
    /// <summary>
    /// Asynchronously send a data to a YDB Topic.
    /// </summary>
    /// <param name="data">The data to produce.</param>
    /// <param name="cancellationToken">A cancellation token to observe whilst waiting the returned task to complete.</param>
    /// <returns>
    /// A Task which will complete with a delivery report corresponding to the produce request,
    /// or an exception if an error occured.
    /// </returns>
    /// <exception cref="T:Ydb.Sdk.Services.Topic.WriterException">
    /// Thrown in response to any write request that was unsuccessful for any reason.
    /// </exception>
    public Task<WriteResult> WriteAsync(TValue data, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously send a single message to a YDB Topic.
    /// </summary>
    /// <param name="message">The message to produce</param>
    /// <param name="cancellationToken">A cancellation token to observe whilst waiting the returned task to complete.</param>
    /// <returns>
    /// A Task which will complete with a delivery report corresponding to the produce request,
    /// or an exception if an error occured.
    /// </returns>
    /// <exception cref="T:Ydb.Sdk.Services.Topic.WriterException">
    /// Thrown in response to any write request that was unsuccessful for any reason.
    /// </exception>
    public Task<WriteResult> WriteAsync(Message<TValue> message, CancellationToken cancellationToken = default);
}
