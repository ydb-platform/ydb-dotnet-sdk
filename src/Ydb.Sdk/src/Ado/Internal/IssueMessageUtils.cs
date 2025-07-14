using System.Text;
using Ydb.Issue;

namespace Ydb.Sdk.Ado.Internal;

internal static class IssueMessageUtils
{
    internal static string IssuesToString(this IReadOnlyList<IssueMessage> issues) => IssuesToString(issues, 0, 4);

    private static string IssuesToString(IEnumerable<IssueMessage> issueMessages, int currentIndent, int indent) =>
        string.Join(Environment.NewLine, issueMessages.Select(message =>
        {
            var sb = new StringBuilder();
            sb.Append(' ', currentIndent);
            sb.Append($"[{message.IssueCode}] ");

            if (message.Position != null)
            {
                sb.Append(message.Position.PositionToString());
            }

            sb.Append($"{message.Severity.SeverityToString()}: ");
            sb.Append(message.Message);

            if (message.Issues.Count > 0)
            {
                sb.AppendLine();
                sb.Append(IssuesToString(message.Issues, currentIndent + indent, indent));
            }

            return sb.ToString();
        }));

    private static string SeverityToString(this uint severity) => severity switch
    {
        0 => "Fatal",
        1 => "Error",
        2 => "Warning",
        3 => "Info",
        _ => $"Unknown SeverityCode {severity}"
    };

    private static string PositionToString(this IssueMessage.Types.Position position)
    {
        var sb = new StringBuilder();
        sb.Append('(');

        if (!string.IsNullOrEmpty(position.File))
        {
            sb.Append(position.File);
            sb.Append(':');
        }

        sb.Append($"{position.Row}:{position.Column}");
        sb.Append(") ");
        return sb.ToString();
    }
}
