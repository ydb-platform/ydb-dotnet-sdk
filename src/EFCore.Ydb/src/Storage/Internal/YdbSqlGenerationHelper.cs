using System.Text;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Storage.Internal;

public class YdbSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies)
    : RelationalSqlGenerationHelper(dependencies)
{
    public override void DelimitIdentifier(StringBuilder builder, string identifier) =>
        builder.Append('`').Append(identifier).Append('`');

    public override string DelimitIdentifier(string identifier) => $"`{identifier}`";

    public override string EscapeIdentifier(string identifier) => identifier.Replace("`", "``");

    public override void EscapeIdentifier(StringBuilder builder, string identifier)
    {
        var length = builder.Length;
        builder.Append(identifier);
        builder.Replace("`", "``", length, identifier.Length);
    }

    public override string DelimitIdentifier(string name, string? schema) =>
        DelimitIdentifier(
            (!string.IsNullOrEmpty(schema) ? schema + "/" : string.Empty)
            + name
        );

    public override void DelimitIdentifier(StringBuilder builder, string name, string? schema) =>
        builder.Append(
            DelimitIdentifier((!string.IsNullOrEmpty(schema) ? schema + "/" : string.Empty) + name)
        );
}
