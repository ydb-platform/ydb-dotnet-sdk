namespace Ydb.Sdk.Ado.TxWriter;

/// <summary>
/// Exception thrown when a transactional topic writer operation fails.
/// </summary>
public class TxTopicWriterException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="TxTopicWriterException"/> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    public TxTopicWriterException(string message) : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="TxTopicWriterException"/> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public TxTopicWriterException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
