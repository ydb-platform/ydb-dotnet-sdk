using System.Data;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using ArgumentOutOfRangeException = System.ArgumentOutOfRangeException;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public class YdbSqlTranslatingExpressionVisitor(
    RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
    QueryCompilationContext queryCompilationContext,
    QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor
) : RelationalSqlTranslatingExpressionVisitor(
    dependencies,
    queryCompilationContext,
    queryableMethodTranslatingExpressionVisitor)
{
    private readonly QueryCompilationContext _queryCompilationContext = queryCompilationContext;

    private readonly YdbSqlExpressionFactory _sqlExpressionFactory =
        (YdbSqlExpressionFactory)dependencies.SqlExpressionFactory;

    private static readonly MethodInfo StringStartsWithMethod
        = typeof(string).GetRuntimeMethod(nameof(string.StartsWith), [typeof(string)])!;

    private static readonly MethodInfo StringEndsWithMethod
        = typeof(string).GetRuntimeMethod(nameof(string.EndsWith), [typeof(string)])!;

    private static readonly MethodInfo StringContainsMethod
        = typeof(string).GetRuntimeMethod(nameof(string.Contains), [typeof(string)])!;

    private static readonly MethodInfo EscapeLikePatternParameterMethod =
        typeof(YdbSqlTranslatingExpressionVisitor).GetTypeInfo()
            .GetDeclaredMethod(nameof(ConstructLikePatternParameter))!;
    
    private static readonly MethodInfo ILike2MethodInfo
        = typeof(YdbFunctionExtension).GetRuntimeMethod(
            nameof(YdbFunctionExtension.ILike), [typeof(DbFunctions), typeof(string), typeof(string)])!;


    protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
    {
        var method = methodCallExpression.Method;

        if (method == StringStartsWithMethod
            && TryTranslateStartsEndsWithContains(
                methodCallExpression.Object!,
                methodCallExpression.Arguments[0],
                StartsEndsWithContains.StartsWith,
                out var translation1)
           )
        {
            return translation1;
        }

        if (method == StringEndsWithMethod
            && TryTranslateStartsEndsWithContains(
                methodCallExpression.Object!,
                methodCallExpression.Arguments[0],
                StartsEndsWithContains.EndsWith,
                out var translation2)
           )
        {
            return translation2;
        }

        if (method == StringContainsMethod
            && TryTranslateStartsEndsWithContains(
                methodCallExpression.Object!,
                methodCallExpression.Arguments[0],
                StartsEndsWithContains.Contains,
                out var translation3)
           )
        {
            return translation3;
        }

        return base.VisitMethodCall(methodCallExpression);
    }

    private bool TryTranslateStartsEndsWithContains(
        Expression instance,
        Expression pattern,
        StartsEndsWithContains methodType,
        [NotNullWhen(true)] out SqlExpression? translation
    )
    {
        if (Visit(instance) is not SqlExpression translatedInstance
            || Visit(pattern) is not SqlExpression translatedPattern)
        {
            translation = null;
            return false;
        }

        var stringTypeMapping = ExpressionExtensions.InferTypeMapping(translatedInstance, translatedPattern);

        // UTF8 is DbType.String whereas STRING is DbType.Binary
        var isUtf8 = stringTypeMapping?.DbType == DbType.String;

        translatedInstance = _sqlExpressionFactory.ApplyTypeMapping(translatedInstance, stringTypeMapping);
        translatedPattern = _sqlExpressionFactory.ApplyTypeMapping(translatedPattern, stringTypeMapping);

        switch (translatedPattern)
        {
            case SqlConstantExpression patternConstant:
            {
                translation = patternConstant.Value switch
                {
                    null => _sqlExpressionFactory.Like(
                        translatedInstance,
                        _sqlExpressionFactory.Constant(null, typeof(string), stringTypeMapping)
                    ),
                    "" => _sqlExpressionFactory.Like(translatedInstance, _sqlExpressionFactory.Constant("%")),
                    string s => _sqlExpressionFactory.Like(
                        translatedInstance,
                        _sqlExpressionFactory.Constant(
                            methodType switch
                            {
                                StartsEndsWithContains.StartsWith => EscapeLikePattern(s) + '%',
                                StartsEndsWithContains.EndsWith => '%' + EscapeLikePattern(s),
                                StartsEndsWithContains.Contains => $"%{EscapeLikePattern(s)}%",

                                _ => throw new ArgumentOutOfRangeException(nameof(methodType), methodType, null)
                            })),

                    _ => throw new UnreachableException()
                };

                return true;
            }

            case SqlParameterExpression patternParameter:
            {
                var lambda = Expression.Lambda(
                    Expression.Call(
                        EscapeLikePatternParameterMethod,
                        QueryCompilationContext.QueryContextParameter,
                        Expression.Constant(patternParameter.Name),
                        Expression.Constant(methodType)),
                    QueryCompilationContext.QueryContextParameter);

                var escapedPatternParameter =
                    _queryCompilationContext.RegisterRuntimeParameter(
                        $"{patternParameter.Name}_{methodType.ToString().ToLower(CultureInfo.InvariantCulture)}",
                        lambda);

                translation = _sqlExpressionFactory.Like(
                    translatedInstance,
                    new SqlParameterExpression(escapedPatternParameter.Name!, escapedPatternParameter.Type,
                        stringTypeMapping));

                return true;
            }

            default:
                switch (methodType)
                {
                    case StartsEndsWithContains.StartsWith or StartsEndsWithContains.EndsWith:
                        var substringArguments = new SqlExpression[3];
                        substringArguments[0] = translatedInstance;
                        substringArguments[2] = _sqlExpressionFactory.Function(
                            "LENGTH",
                            [translatedPattern],
                            nullable: true,
                            argumentsPropagateNullability: [true],
                            typeof(int)
                        );

                        if (methodType == StartsEndsWithContains.StartsWith)
                        {
                            substringArguments[1] = _sqlExpressionFactory.Constant(1);
                        }
                        else
                        {
                            substringArguments[1] = _sqlExpressionFactory.Subtract(
                                _sqlExpressionFactory.Function(
                                    "LENGTH",
                                    [translatedInstance],
                                    nullable: true,
                                    argumentsPropagateNullability: [true],
                                    typeof(int)
                                ),
                                _sqlExpressionFactory.Function(
                                    "LENGTH",
                                    [translatedPattern],
                                    nullable: true,
                                    argumentsPropagateNullability: [true],
                                    typeof(int)
                                )
                            );
                        }

                        var substringFunction = _sqlExpressionFactory.Function(
                            "SUBSTRING",
                            substringArguments,
                            nullable: true,
                            argumentsPropagateNullability: [true, false, false],
                            typeof(string),
                            stringTypeMapping
                        );

                        translation = _sqlExpressionFactory.AndAlso(
                            _sqlExpressionFactory.IsNotNull(translatedInstance),
                            _sqlExpressionFactory.AndAlso(
                                _sqlExpressionFactory.IsNotNull(translatedPattern),
                                _sqlExpressionFactory.OrElse(
                                    _sqlExpressionFactory.Equal(
                                        isUtf8
                                            ? _sqlExpressionFactory.Function(
                                                "unwrap",
                                                [
                                                    _sqlExpressionFactory.Convert(
                                                        substringFunction,
                                                        typeof(string),
                                                        typeMapping: StringTypeMapping.Default
                                                    )
                                                ],
                                                nullable: false,
                                                argumentsPropagateNullability: [true],
                                                typeof(string)
                                            )
                                            : substringFunction,
                                        translatedPattern
                                    ),
                                    _sqlExpressionFactory.Equal(translatedPattern,
                                        _sqlExpressionFactory.Constant(string.Empty)
                                    )
                                )
                            )
                        );
                        break;
                    case StartsEndsWithContains.Contains:
                        translation =
                            _sqlExpressionFactory.AndAlso(
                                _sqlExpressionFactory.IsNotNull(translatedInstance),
                                _sqlExpressionFactory.AndAlso(
                                    _sqlExpressionFactory.IsNotNull(translatedPattern),
                                    _sqlExpressionFactory.GreaterThan(
                                        _sqlExpressionFactory.Function(
                                            "strpos", [translatedInstance, translatedPattern], nullable: true,
                                            argumentsPropagateNullability: [true, true], typeof(int)),
                                        _sqlExpressionFactory.Constant(0))));
                        break;

                    default:
                        throw new UnreachableException();
                }

                return true;
        }
    }


    public enum StartsEndsWithContains
    {
        StartsWith,
        EndsWith,
        Contains
    }

    public static string? ConstructLikePatternParameter(
        QueryContext queryContext,
        string baseParameterName,
        StartsEndsWithContains methodType
    )
        => queryContext.ParameterValues[baseParameterName] switch
        {
            null => null,

            // In .NET, all strings start/end with the empty string, but SQL LIKE return false for empty patterns.
            // Return % which always matches instead.
            "" => "%",

            string s => methodType switch
            {
                StartsEndsWithContains.StartsWith => EscapeLikePattern(s) + '%',
                StartsEndsWithContains.EndsWith => '%' + EscapeLikePattern(s),
                StartsEndsWithContains.Contains => $"%{EscapeLikePattern(s)}%",
                _ => throw new ArgumentOutOfRangeException(nameof(methodType), methodType, null)
            },

            _ => throw new UnreachableException()
        };

    private const char LikeEscapeChar = '\\';

    private static bool IsLikeWildChar(char c)
        => c is '%' or '_';

    private static string EscapeLikePattern(string pattern)
    {
        var builder = new StringBuilder();
        foreach (var c in pattern)
        {
            if (IsLikeWildChar(c) || c == LikeEscapeChar)
            {
                builder.Append(LikeEscapeChar);
            }

            builder.Append(c);
        }

        return builder.ToString();
    }
}
