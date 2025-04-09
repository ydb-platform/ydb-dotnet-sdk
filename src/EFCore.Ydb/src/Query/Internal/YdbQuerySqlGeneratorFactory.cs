using Microsoft.EntityFrameworkCore.Query;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public class YdbQuerySqlGeneratorFactory(QuerySqlGeneratorDependencies dependencies) : IQuerySqlGeneratorFactory
{
    public QuerySqlGenerator Create() => new YdbQuerySqlGenerator(dependencies);
}
