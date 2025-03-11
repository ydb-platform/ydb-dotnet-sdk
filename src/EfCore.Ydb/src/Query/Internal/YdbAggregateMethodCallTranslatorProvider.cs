using EfCore.Ydb.Query.Internal.Translators;
using Microsoft.EntityFrameworkCore.Query;

namespace EfCore.Ydb.Query.Internal;

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
            new YdbQueryableAggregateMethodTranslator(sqlExpressionFactory, dependencies.RelationalTypeMappingSource)
        ]);
    }
}
