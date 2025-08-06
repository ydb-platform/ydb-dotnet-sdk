using System.Collections.Generic;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EntityFrameworkCore.Ydb.Query.Internal.Translators;

public class YdbILikeFunctionTranslator(YdbSqlExpressionFactory sqlExpressionFactory) : IMethodCallTranslator
{
    // todo: disable эту хуету с неймингом
    private static readonly MethodInfo ILike =
        typeof(DbFunctionsExtensions).GetRuntimeMethod(
            nameof(YdbFunctionExtension.ILike),
            [typeof(DbFunctions), typeof(string), typeof(string)])!;
    
    private static readonly MethodInfo ILikeWithEscape =
        typeof(DbFunctionsExtensions).GetRuntimeMethod(
            nameof(YdbFunctionExtension.ILike),
            [typeof(DbFunctions), typeof(string), typeof(string), typeof(string)])!;
    

    /// <inheritdoc />
    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        (SqlExpression match, SqlExpression pattern) = (arguments[1], arguments[2]);
        
        if (method == ILikeWithEscape)
        {
            return sqlExpressionFactory.ILike(match, pattern, arguments[3]);
        }
        
        return sqlExpressionFactory.ILike(match, pattern, sqlExpressionFactory.Constant(string.Empty));
    }
}
