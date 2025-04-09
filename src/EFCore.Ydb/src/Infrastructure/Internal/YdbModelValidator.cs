using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EntityFrameworkCore.Ydb.Infrastructure.Internal;

public class YdbModelValidator(
    ModelValidatorDependencies dependencies,
    RelationalModelValidatorDependencies relationalDependencies
) : RelationalModelValidator(dependencies, relationalDependencies);
