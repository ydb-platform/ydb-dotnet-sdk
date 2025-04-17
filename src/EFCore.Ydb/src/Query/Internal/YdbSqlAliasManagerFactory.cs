using System.Diagnostics.CodeAnalysis;
using Microsoft.EntityFrameworkCore.Query;

namespace EntityFrameworkCore.Ydb.Query.Internal;

[Experimental("EF9002")]
public class YdbSqlAliasManagerFactory : ISqlAliasManagerFactory
{
    public SqlAliasManager Create() 
        => new YdbSqlAliasManager();
}
