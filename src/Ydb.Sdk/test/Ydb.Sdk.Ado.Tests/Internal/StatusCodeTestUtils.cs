using Xunit;
using Ydb.Issue;
using Ydb.Sdk.Ado.Internal;

namespace Ydb.Sdk.Ado.Tests.Internal;

public class StatusCodeTestUtils
{
    [Theory]
    [InlineData(Grpc.Core.StatusCode.Unavailable, StatusCode.ClientTransportUnavailable)]
    [InlineData(Grpc.Core.StatusCode.DeadlineExceeded, StatusCode.ClientTransportTimeout)]
    [InlineData(Grpc.Core.StatusCode.ResourceExhausted, StatusCode.ClientTransportResourceExhausted)]
    [InlineData(Grpc.Core.StatusCode.Unimplemented, StatusCode.ClientTransportUnimplemented)]
    [InlineData(Grpc.Core.StatusCode.Cancelled, StatusCode.ClientTransportTimeout)]
    public void Code_GrpcCoreStatusCodeConvertToStatusCode_Assert(
        Grpc.Core.StatusCode statusCode,
        StatusCode expectedStatusCode
    ) => Assert.Equal(expectedStatusCode, new Grpc.Core.Status(statusCode, "Mock status").Code());


    [Fact]
    public void ServerMessage_WhenServerSendsEmptyIssues_Assert() =>
        Assert.Equal("Status: Aborted", StatusIds.Types.StatusCode.Aborted.Code().ToMessage([]));

    [Fact]
    public void ServerMessage_WhenServerSendsListIssues_Assert() => Assert.Equal(
        """
        Status: BadSession, Issues:
        [0] Fatal: Session is bad :(
        [1] Error: Session is very bad :(
        [2] (good.txt:2:2) Warning: Session is very bad :((
        [1000] (1:1) Info: Session is very bad :)
        [2000] Unknown SeverityCode 10: Unknown severity test :)
        """,
        StatusIds.Types.StatusCode.BadSession.Code().ToMessage(new List<IssueMessage>
        {
            new() { IssueCode = 0, Severity = 0, Message = "Session is bad :(" },
            new() { IssueCode = 1, Severity = 1, Message = "Session is very bad :(" },
            new()
            {
                IssueCode = 2,
                Position = new IssueMessage.Types.Position { File = "good.txt", Column = 2, Row = 2 },
                Severity = 2,
                Message = "Session is very bad :(("
            },
            new()
            {
                IssueCode = 1000,
                Position = new IssueMessage.Types.Position { Column = 1, Row = 1 },
                Severity = 3,
                Message = "Session is very bad :)"
            },
            new() { IssueCode = 2000, Severity = 10, Message = "Unknown severity test :)" }
        }));

    [Fact]
    public void ServerMessage_WhenServerSendsRecursiveListIssues_Assert()
    {
        var listIssues = new List<IssueMessage>();
        var recursiveIssue = new IssueMessage
        {
            IssueCode = 0, Severity = 0, Message = "Overloaded is bad :("
        };
        var recursiveIssueInRecIssue = new IssueMessage
        {
            IssueCode = 1, Severity = 1, Message = "Overloaded is very bad :("
        };
        recursiveIssueInRecIssue.Issues.Add(new IssueMessage
        {
            IssueCode = 2,
            Position = new IssueMessage.Types.Position { File = "good.txt", Column = 2, Row = 2 },
            Severity = 2,
            Message = "Overloaded is very bad :(("
        });
        recursiveIssue.Issues.Add(recursiveIssueInRecIssue);
        recursiveIssue.Issues.Add(new IssueMessage
        {
            IssueCode = 1000,
            Position = new IssueMessage.Types.Position { Column = 1, Row = 1 },
            Severity = 3,
            Message = "Overloaded is very bad :)"
        });
        recursiveIssue.Issues.Add(new IssueMessage
            { IssueCode = 2000, Severity = 10, Message = "Unknown severity test :)" });
        listIssues.Add(recursiveIssue);


        Assert.Equal(
            """
            Status: Overloaded, Issues:
            [0] Fatal: Overloaded is bad :(
                [1] Error: Overloaded is very bad :(
                    [2] (good.txt:2:2) Warning: Overloaded is very bad :((
                [1000] (1:1) Info: Overloaded is very bad :)
                [2000] Unknown SeverityCode 10: Unknown severity test :)
            """,
            StatusIds.Types.StatusCode.Overloaded.Code().ToMessage(listIssues)
        );
    }
}
