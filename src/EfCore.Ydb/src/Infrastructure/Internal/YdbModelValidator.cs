using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EfCore.Ydb.Infrastructure.Internal;

// TODO: Not required for mvp
public class YdbModelValidator(
    ModelValidatorDependencies dependencies,
    RelationalModelValidatorDependencies relationalDependencies
) : RelationalModelValidator(dependencies, relationalDependencies);
