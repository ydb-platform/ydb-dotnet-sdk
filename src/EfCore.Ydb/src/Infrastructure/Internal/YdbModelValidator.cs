using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EfCore.Ydb.Infrastructure.Internal;

// TODO: Not required for mvp
public class YdbModelValidator : RelationalModelValidator
{
    public YdbModelValidator(
        ModelValidatorDependencies dependencies,
        RelationalModelValidatorDependencies relationalDependencies
    ) : base(dependencies, relationalDependencies)
    {
    }
}
