using EfCore.Ydb.Query.Internal.Translators;
using Microsoft.EntityFrameworkCore.Query;

namespace EfCore.Ydb.Query.Internal;

public class YdbMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    public YdbMemberTranslatorProvider(RelationalMemberTranslatorProviderDependencies dependencies) : base(dependencies)
    {
        var sqlExpressionFactory = (YdbSqlExpressionFactory)dependencies.SqlExpressionFactory;

        AddTranslators(
        [
            new StubTranslator(sqlExpressionFactory)
        ]);
    }
}
