using EfCore.Ydb.Query.Internal.Translators;
using Microsoft.EntityFrameworkCore.Query;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public sealed class YdbMethodCallTranslatorProvider : RelationalMethodCallTranslatorProvider
{
    public YdbMethodCallTranslatorProvider(
        RelationalMethodCallTranslatorProviderDependencies dependencies
    ) : base(dependencies)
    {
        AddTranslators(
            [
                new StubTranslator()
            ]
        );
    }
}
