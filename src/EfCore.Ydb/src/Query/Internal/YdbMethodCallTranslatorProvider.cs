using EfCore.Ydb.Query.Internal.Translators;
using Microsoft.EntityFrameworkCore.Query;

namespace EfCore.Ydb.Query.Internal;

public class YdbMethodCallTranslatorProvider : RelationalMethodCallTranslatorProvider
{
    public YdbMethodCallTranslatorProvider(
        RelationalMethodCallTranslatorProviderDependencies dependencies
    ) : base(dependencies)
    {
        var sqlExpressionFactory = (YdbSqlExpressionFactory)dependencies.SqlExpressionFactory;
        AddTranslators(
        [
            new StubTranslator(sqlExpressionFactory)
        ]);
    }
}
