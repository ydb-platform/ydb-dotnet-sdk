using EntityFrameworkCore.Ydb.Query.Internal.Translators;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public sealed class YdbMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    public YdbMemberTranslatorProvider(
        RelationalMemberTranslatorProviderDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource
    ) : base(dependencies)
    {
        var sqlExpressionFactory = (YdbSqlExpressionFactory)dependencies.SqlExpressionFactory;

        AddTranslators(
            [
                new YdbDateTimeMemberTranslator(typeMappingSource, sqlExpressionFactory)
            ]
        );
    }
}
