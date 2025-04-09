using EntityFrameworkCore.Ydb.Query.Internal;
using Microsoft.EntityFrameworkCore.Query;

namespace EfCore.Ydb.Query.Internal;

public class YdbQuerySqlGeneratorFactory(QuerySqlGeneratorDependencies dependencies) : IQuerySqlGeneratorFactory
{
    public QuerySqlGenerator Create() => new YdbQuerySqlGenerator(dependencies);
}
