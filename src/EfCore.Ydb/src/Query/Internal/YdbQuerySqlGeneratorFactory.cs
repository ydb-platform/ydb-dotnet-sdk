using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

namespace EfCore.Ydb.Query.Internal;

public class YdbQuerySqlGeneratorFactory : IQuerySqlGeneratorFactory
{
    private readonly QuerySqlGeneratorDependencies _dependencies;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public YdbQuerySqlGeneratorFactory(
        QuerySqlGeneratorDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource
    )
    {
        _dependencies = dependencies;
        _typeMappingSource = typeMappingSource;
    }

    public QuerySqlGenerator Create()
        => new YdbQuerySqlGenerator(_dependencies, _typeMappingSource);
}
