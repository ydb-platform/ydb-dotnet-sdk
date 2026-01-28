namespace Ydb.Sdk.Ado.TxWriter;

/// <summary>
/// Represents a transactional topic writer that enqueues messages to be sent within the current transaction.
/// </summary>
/// <typeparam name="T">The type of values to write to the topic.</typeparam>
/// <remarks>
/// Messages written via this interface are bound to the current transaction and become visible
/// atomically together with table changes after a successful commit. Message sending is performed
/// in the background while the application continues to work with tables or other topics.
/// Before committing, the connection waits for acknowledgements of all pending messages.
/// </remarks>
public interface ITxTopicWriter<in T>
{
    /// <summary>
    /// Enqueues a message for delivery within the current transaction.
    /// </summary>
    /// <param name="value">The value to write to the topic.</param>
    /// <exception cref="InvalidOperationException">Thrown when the transaction is not active.</exception>
    /// <exception cref="TxTopicWriterException">
    /// Thrown when the buffer is full and cannot accept more messages.
    /// The caller should flush pending messages or implement backpressure.
    /// </exception>
    void Write(T value);
}
