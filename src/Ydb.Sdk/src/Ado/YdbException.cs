using System.Data.Common;
using Grpc.Core;
using Ydb.Issue;
using Ydb.Sdk.Ado.Internal;

namespace Ydb.Sdk.Ado;

/// <summary>
/// The exception that is thrown when a YDB operation fails.
/// </summary>
/// <remarks>
/// YdbException is thrown when YDB operations encounter errors.
/// It provides access to YDB-specific error codes and issues.
/// Purely Ydb.Sdk-related issues which aren't related to the server will be raised
/// via the standard CLR exceptions (e.g. ArgumentException).
/// </remarks>
public class YdbException : DbException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="YdbException"/> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    internal YdbException(string message) : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbException"/> class from an RPC exception.
    /// </summary>
    /// <param name="e">The <see cref="RpcException"/> that caused this YDB exception.</param>
    internal YdbException(RpcException e) : this(e.Status.Code(), "Transport RPC call error", e)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbException"/> class from server response status and issues.
    /// </summary>
    /// <param name="statusCode">The status code returned by the YDB server.</param>
    /// <param name="issues">The list of issues returned by the YDB server.</param>
    internal static YdbException FromServer(StatusIds.Types.StatusCode statusCode, IReadOnlyList<IssueMessage> issues)
    {
        var code = statusCode.Code();
        var message = code.ToMessage(issues);

        return new YdbException(code, message);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbException"/> class from server operation.
    /// </summary>
    internal static YdbException FromServer(Operations.Operation operation) =>
        FromServer(operation.Status, operation.Issues);

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbException"/> class with a specified status code, error message,
    /// and optional inner exception.
    /// </summary>
    /// <param name="statusCode">The YDB status code associated with this exception.</param>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception
    /// or null if no inner exception is specified.</param>
    internal YdbException(StatusCode statusCode, string message, Exception? innerException = null)
        : base(message, innerException)
    {
        Code = statusCode;
        IsTransient = statusCode.IsTransient();
        // TODO: Add SQLSTATE message with order with https://en.wikipedia.org/wiki/SQLSTATE
    }

    /// <inheritdoc />
    public override bool IsTransient { get; }

    /// <summary>
    /// Gets the YDB status code associated with this exception.
    /// </summary>
    /// <remarks>
    /// The status code provides detailed information about the type of error that occurred.
    /// This can be used to determine the appropriate error handling strategy.
    /// </remarks>
    public StatusCode Code { get; }
}

/// <summary>
/// The exception that is thrown when an operation is attempted on a <see cref="YdbConnection"/>
/// that already has an operation in progress.
/// </summary>
/// <remarks>
/// YdbOperationInProgressException is thrown when attempting to execute a command on a <see cref="YdbConnection"/>
/// that is already executing another command. <see cref="YdbConnection"/> does not support concurrent operations.
/// </remarks>
public class YdbOperationInProgressException : InvalidOperationException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="YdbOperationInProgressException"/> class.
    /// </summary>
    /// <param name="ydbConnection">The <see cref="YdbConnection"/> that has an operation in progress.</param>
    internal YdbOperationInProgressException(YdbConnection ydbConnection)
        : base("A command is already in progress: " + ydbConnection.LastCommand)
    {
    }
}
