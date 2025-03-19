using System.Text;
using Microsoft.EntityFrameworkCore.Storage;

namespace EfCore.Ydb.Storage.Internal;

public class YdbSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies)
    : RelationalSqlGenerationHelper(dependencies)
{
    public override void DelimitIdentifier(StringBuilder builder, string identifier) =>
        builder.Append('`').Append(identifier).Append('`');

    public override string DelimitIdentifier(string identifier) => $"`{identifier}`";
}
