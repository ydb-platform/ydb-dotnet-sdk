using System.Collections.Generic;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EntityFrameworkCore.Ydb.Query.Internal.Translators;

public class YdbILikeFunctionTranslator(YdbSqlExpressionFactory sqlExpressionFactory) : IMethodCallTranslator
{
    //TODO: disable эту хуету с неймингом
    //TODO: fix get naming
    private static readonly MethodInfo ILike =
        typeof(DbFunctionsExtensions).GetRuntimeMethod(
            "Like",
            [typeof(DbFunctions), typeof(string), typeof(string)])!;
    
    //TODO: fix get naming
    private static readonly MethodInfo ILikeWithEscape =
        typeof(DbFunctionsExtensions).GetRuntimeMethod(
            "Like",
            [typeof(DbFunctions), typeof(string), typeof(string), typeof(string)])!;
    

    /// <inheritdoc />
    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        //var methods = typeof(DbFunctionsExtensions).GetMethods();
        //var mathodName = nameof(YdbFunctionExtension.ILike);
        
        (SqlExpression match, SqlExpression pattern) = (arguments[1], arguments[2]);
        
        //TODO: тут происходит какая-то ебатория с именами (Like и ILike)
        if (method == ILikeWithEscape)
        {
            return sqlExpressionFactory.ILike(match, pattern, arguments[3]);
        }
        
        return sqlExpressionFactory.ILike(match, pattern);
    }
}
