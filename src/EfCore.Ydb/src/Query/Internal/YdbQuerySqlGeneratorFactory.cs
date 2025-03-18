using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

namespace EfCore.Ydb.Query.Internal;

public class YdbQuerySqlGeneratorFactory(
    QuerySqlGeneratorDependencies dependencies,
    IRelationalTypeMappingSource typeMappingSource
) : IQuerySqlGeneratorFactory
{
    public QuerySqlGenerator Create() => new YdbQuerySqlGenerator(dependencies, typeMappingSource);
}
