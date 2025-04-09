using EfCore.Ydb.Query.Internal;
using EfCore.Ydb.Query.Internal.Translators;
using Microsoft.EntityFrameworkCore.Query;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public sealed class YdbAggregateMethodCallTranslatorProvider
    : RelationalAggregateMethodCallTranslatorProvider
{
    public YdbAggregateMethodCallTranslatorProvider(
        RelationalAggregateMethodCallTranslatorProviderDependencies dependencies
    ) : base(dependencies)
    {
        var sqlExpressionFactory = (YdbSqlExpressionFactory)dependencies.SqlExpressionFactory;

        AddTranslators(
            [
                new YdbQueryableAggregateMethodTranslator(
                    sqlExpressionFactory,
                    dependencies.RelationalTypeMappingSource
                )
            ]
        );
    }
}
