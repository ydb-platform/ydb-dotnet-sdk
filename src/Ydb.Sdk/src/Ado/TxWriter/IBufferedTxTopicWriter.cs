namespace Ydb.Sdk.Ado.TxWriter;

/// <summary>
/// Internal base interface for all transactional topic writers.
/// </summary>
internal interface IInternalTxWriter : IDisposableTxWriter
{
    /// <summary>
    /// Waits for all enqueued messages to be durably accepted within the current transaction.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token to observe while waiting.</param>
    /// <returns>A task representing the asynchronous flush operation.</returns>
    Task FlushAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Internal interface for transactional topic writers with flush support.
/// </summary>
/// <typeparam name="T">The type of values to write to the topic.</typeparam>
internal interface IBufferedTxTopicWriter<in T> : ITxTopicWriter<T>, IInternalTxWriter
{
}

/// <summary>
/// Internal interface for disposing TxWriters.
/// </summary>
internal interface IDisposableTxWriter
{
    /// <summary>
    /// Disposes the TxWriter and cleans up resources.
    /// </summary>
    void Dispose();
}
