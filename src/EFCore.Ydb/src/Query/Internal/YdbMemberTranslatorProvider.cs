using EfCore.Ydb.Query.Internal.Translators;
using Microsoft.EntityFrameworkCore.Query;

namespace EntityFrameworkCore.Ydb.Query.Internal;

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
