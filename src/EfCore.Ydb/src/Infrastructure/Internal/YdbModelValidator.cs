using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EfCore.Ydb.Infrastructure.Internal;

public class YdbModelValidator(
    ModelValidatorDependencies dependencies,
    RelationalModelValidatorDependencies relationalDependencies
) : RelationalModelValidator(dependencies, relationalDependencies);
