using System.Text;
using Google.Protobuf.Collections;
using Ydb.Issue;

namespace Ydb.Sdk;

internal static class StatusRanges
{
    public const uint ClientFirst = 500000;
    public const uint ClientTransportFirst = 600000;
}

public enum StatusCode : uint
{
    Unspecified = 0,
    Success = 400000,
    BadRequest = 400010,
    Unauthorized = 400020,
    InternalError = 400030,
    Aborted = 400040,
    Unavailable = 400050,
    Overloaded = 400060,
    SchemeError = 400070,
    GenericError = 400080,
    Timeout = 400090,
    BadSession = 400100,
    PreconditionFailed = 400120,
    AlreadyExists = 400130,
    NotFound = 400140,
    SessionExpired = 400150,
    Cancelled = 400160,
    Undetermined = 400170,
    Unsupported = 400180,
    SessionBusy = 400190,

    ClientResourceExhausted = StatusRanges.ClientFirst + 10,
    ClientInternalError = StatusRanges.ClientFirst + 20,

    ClientTransportUnknown = StatusRanges.ClientTransportFirst + 10,
    ClientTransportUnavailable = StatusRanges.ClientTransportFirst + 20,
    ClientTransportTimeout = StatusRanges.ClientTransportFirst + 30,
    ClientTransportResourceExhausted = StatusRanges.ClientTransportFirst + 40,
    ClientTransportUnimplemented = StatusRanges.ClientTransportFirst + 50
}

public enum IssueSeverity
{
    Unknown = -1,
    Fatal = 0,
    Error = 1,
    Warning = 2,
    Info = 3
}

public readonly struct Position
{
    private string? File { get; }
    private uint Row { get; }
    private uint Column { get; }

    public Position(string? file, uint row, uint column)
    {
        File = file;
        Row = row;
        Column = column;
    }

    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append('(');

        if (File != null)
        {
            sb.Append(File);
            sb.Append(':');
        }

        sb.Append(Row);
        sb.Append(':');
        sb.Append(Column);
        sb.Append(')');

        return sb.ToString();
    }
}

public class Issue
{
    public uint IssueCode { get; }

    private IssueSeverity Severity { get; } = IssueSeverity.Error;

    private Position? Position { get; }
    public string Message { get; }

    private IReadOnlyList<Issue> Children { get; } = Array.Empty<Issue>();

    internal Issue(IssueMessage issue)
    {
        IssueCode = issue.IssueCode;

        Severity = Enum.IsDefined(typeof(IssueSeverity), (int)issue.Severity)
            ? (IssueSeverity)issue.Severity
            : IssueSeverity.Unknown;

        if (issue.Position != null)
        {
            Position = new Position(issue.Position.File, issue.Position.Row, issue.Position.Column);
        }

        Message = issue.Message;
        Children = issue.Issues
            .Select(i => new Issue(i))
            .ToList();
    }

    public Issue(string message)
    {
        Message = message;
    }

    public override string ToString() => ToString(0, 4);

    private string ToString(int currentIndent, int indent)
    {
        var sb = new StringBuilder();
        sb.Append(new string(' ', currentIndent));
        sb.Append($"[{IssueCode}] ");

        if (Position != null)
        {
            sb.Append(Position);
        }

        sb.Append($"{Severity}: ");
        sb.Append(Message);
        sb.Append(IssuesToString(Children, currentIndent + indent, indent));
        return sb.ToString();
    }

    internal static string IssuesToString(IReadOnlyList<Issue> issues) => IssuesToString(issues, 0, 4);

    private static string IssuesToString(IReadOnlyList<Issue> issues, int currentIndent, int indent)
    {
        var sb = new StringBuilder();

        foreach (var issue in issues)
        {
            sb.Append(issue.ToString(currentIndent, indent));
            sb.Append(Environment.NewLine);
        }

        return sb.ToString();
    }
}

public class Status
{
    public static readonly Status Success = new(StatusCode.Success);

    public StatusCode StatusCode { get; }
    public IReadOnlyList<Issue> Issues { get; }

    internal Status(StatusCode statusCode, IReadOnlyList<Issue> issues)
    {
        StatusCode = statusCode;
        Issues = issues;
    }

    internal Status(StatusCode statusCode) : this(statusCode, Array.Empty<Issue>())
    {
    }

    internal Status(StatusCode statusCode, string message) : this(statusCode, new List<Issue> { new(message) })
    {
    }

    public bool IsSuccess => StatusCode == StatusCode.Success;
    public bool IsNotSuccess => !IsSuccess;

    public void EnsureSuccess()
    {
        if (!IsSuccess)
        {
            throw new StatusUnsuccessfulException(this);
        }
    }

    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append($"Status: {StatusCode}");

        if (Issues.Count == 0)
        {
            return sb.ToString();
        }

        sb.Append(", Issues:");
        sb.Append(Environment.NewLine);
        sb.Append(Issue.IssuesToString(Issues));

        return sb.ToString();
    }

    private static StatusCode ConvertStatusCode(StatusIds.Types.StatusCode statusCode)
    {
        var value = (uint)statusCode;
        if (Enum.IsDefined(typeof(StatusCode), value))
        {
            return (StatusCode)value;
        }

        return StatusCode.Unspecified;
    }

    public static Status FromProto(StatusIds.Types.StatusCode statusCode, RepeatedField<IssueMessage> issues) =>
        new(ConvertStatusCode(statusCode), issues.Select(i => new Issue(i)).ToList());
}

public class StatusUnsuccessfulException : Exception
{
    public StatusUnsuccessfulException(Status status) : base(status.ToString())
    {
        Status = status;
    }

    public Status Status { get; }
}

internal static class StatusExtensions
{
    internal static Status ConvertStatus(this Grpc.Core.Status rpcStatus) =>
        new(
            rpcStatus.StatusCode switch
            {
                Grpc.Core.StatusCode.Unavailable => StatusCode.ClientTransportUnavailable,
                Grpc.Core.StatusCode.DeadlineExceeded => StatusCode.ClientTransportTimeout,
                Grpc.Core.StatusCode.ResourceExhausted => StatusCode.ClientTransportResourceExhausted,
                Grpc.Core.StatusCode.Unimplemented => StatusCode.ClientTransportUnimplemented,
                Grpc.Core.StatusCode.Cancelled => StatusCode.Cancelled,
                _ => StatusCode.ClientTransportUnknown
            },
            new List<Issue> { new(rpcStatus.Detail) }
        );
}
