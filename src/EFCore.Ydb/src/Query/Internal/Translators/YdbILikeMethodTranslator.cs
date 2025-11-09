using System.Collections.Generic;
using System.Reflection;
using EntityFrameworkCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EntityFrameworkCore.Ydb.Query.Internal.Translators;

public class YdbILikeMethodTranslator: IMethodCallTranslator
{
	private static readonly MethodInfo ILike =
		typeof(YdbDbFunctionsExtension).GetRuntimeMethod(
			nameof(YdbDbFunctionsExtension.ILike),
			[typeof(DbFunctions), typeof(string), typeof(string)]);
	
	private readonly YdbSqlExpressionFactory _sqlExpressionFactory;

	public YdbILikeMethodTranslator(YdbSqlExpressionFactory sqlExpressionFactory)
	{
		_sqlExpressionFactory = sqlExpressionFactory;
	}

	public SqlExpression? Translate(SqlExpression? instance, MethodInfo method, IReadOnlyList<SqlExpression> arguments,
		IDiagnosticsLogger<DbLoggerCategory.Query> logger)
	{
		if (method == ILike)
		{
			return _sqlExpressionFactory.ILike(arguments[1], arguments[2]);
		}

		return null;
	}
}
