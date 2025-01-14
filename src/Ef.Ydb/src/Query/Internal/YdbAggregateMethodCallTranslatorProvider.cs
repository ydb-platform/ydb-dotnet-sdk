using Ef.Ydb.Query.Internal.Translators;
using Microsoft.EntityFrameworkCore.Query;

namespace Ef.Ydb.Query.Internal;

public class YdbAggregateMethodCallTranslatorProvider
    : RelationalAggregateMethodCallTranslatorProvider
{
    public YdbAggregateMethodCallTranslatorProvider(
        RelationalAggregateMethodCallTranslatorProviderDependencies dependencies
    ) : base(dependencies)
    {
        var sqlExpressionFactory = (YdbSqlExpressionFactory)dependencies.SqlExpressionFactory;

        AddTranslators(
        [
            new YdbQueryableAggregateMethodTranslator(sqlExpressionFactory)
        ]);
    }
}
