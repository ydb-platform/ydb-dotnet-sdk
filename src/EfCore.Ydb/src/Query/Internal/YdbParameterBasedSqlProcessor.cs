using Microsoft.EntityFrameworkCore.Query;

namespace EfCore.Ydb.Query.Internal;

public class YdbParameterBasedSqlProcessor(
    RelationalParameterBasedSqlProcessorDependencies dependencies,
    RelationalParameterBasedSqlProcessorParameters parameters
) : RelationalParameterBasedSqlProcessor(dependencies, parameters);
