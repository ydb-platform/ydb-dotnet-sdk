using Microsoft.EntityFrameworkCore.Query;

namespace Ef.Ydb.Query.Internal;

public class YdbQuerySqlGeneratorFactory : IQuerySqlGeneratorFactory
{
    private readonly QuerySqlGeneratorDependencies _dependencies;

    public YdbQuerySqlGeneratorFactory(
        QuerySqlGeneratorDependencies dependencies
    ) => _dependencies = dependencies;

    public QuerySqlGenerator Create()
        => new YdbQuerySqlGenerator(_dependencies);
}
