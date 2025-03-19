using EfCore.Ydb.Query.Internal.Translators;
using Microsoft.EntityFrameworkCore.Query;

namespace EfCore.Ydb.Query.Internal;

public sealed class YdbMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    public YdbMemberTranslatorProvider(RelationalMemberTranslatorProviderDependencies dependencies) : base(dependencies)
    {
        AddTranslators(
            [
                new StubTranslator()
            ]
        );
    }
}
