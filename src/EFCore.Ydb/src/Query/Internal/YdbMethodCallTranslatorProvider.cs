using EntityFrameworkCore.Ydb.Query.Internal.Translators;
using Microsoft.EntityFrameworkCore.Query;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public sealed class YdbMethodCallTranslatorProvider : RelationalMethodCallTranslatorProvider
{
    public YdbMethodCallTranslatorProvider(RelationalMethodCallTranslatorProviderDependencies dependencies) :
        base(dependencies)
    {
        var sqlExpressionFactory = (YdbSqlExpressionFactory)dependencies.SqlExpressionFactory;

        AddTranslators(
            [
                new YdbDateTimeMethodTranslator(sqlExpressionFactory),
                new YdbMathTranslator(sqlExpressionFactory),
                new YdbMathTranslator(sqlExpressionFactory),
                new YdbByteArrayMethodTranslator(sqlExpressionFactory)
            ]
        );
    }
}
