using System.Collections.Concurrent;
using System.Text;

namespace Ydb.Sdk.Ado.Internal;

internal static class SqlParser
{
    private static readonly IDictionary<string, ParsedResult> CacheQueries =
        new ConcurrentDictionary<string, ParsedResult>();

    internal static ParsedResult Parse(string sql)
    {
        if (CacheQueries.TryGetValue(sql, out var preparedYql))
        {
            return preparedYql;
        }

        var newYql = new StringBuilder();
        var sqlParamsBuilder = new SqlParamsBuilder();
        var fragmentToken = 0;

        for (var curToken = 0; curToken < sql.Length;)
        {
            switch (sql[curToken])
            {
                case '`':
                    curToken = SkipTerminals(sql, '`', curToken);
                    break;
                case '"':
                    curToken = SkipTerminals(sql, '"', curToken);
                    break;
                case '\'':
                    curToken = SkipTerminals(sql, '\'', curToken);
                    break;
                case '-':
                    curToken = ParseLineComment(sql, curToken);
                    break;
                case '/':
                    curToken = ParseBlockComment(sql, curToken);
                    break;
                case '@':
                    if (curToken + 1 < sql.Length && sql[curToken + 1] == '@')
                    {
                        curToken = ParseMultilineStringLiterals(sql, curToken);
                        break;
                    }

                    var parsedParam = ParseNameParam(sql, curToken);

                    newYql.Append(sql[fragmentToken .. curToken]).Append(parsedParam.Name);
                    sqlParamsBuilder.AddPrimitiveParam(parsedParam.Name, false);
                    fragmentToken = parsedParam.NextToken;
                    curToken = parsedParam.NextToken;
                    break;
                case '$':
                    var parsedNativeParam = ParseNameParam(sql, curToken);

                    newYql.Append(sql[fragmentToken .. curToken]).Append(parsedNativeParam.Name);
                    sqlParamsBuilder.AddPrimitiveParam(parsedNativeParam.Name, true);
                    fragmentToken = parsedNativeParam.NextToken;
                    curToken = parsedNativeParam.NextToken;
                    break;
                case var _ when ParseInKeyWord(sql, curToken):
                    curToken += 2; // skip IN keyword
                    newYql.Append(sql[fragmentToken .. curToken]);
                    curToken = ParseInListParameters(sql, curToken, sqlParamsBuilder, newYql);
                    fragmentToken = curToken;
                    break;
                default:
                    curToken++;
                    break;
            }
        }

        newYql.Append(sql.AsSpan(fragmentToken, sql.Length - fragmentToken));

        return CacheQueries[sql] = new ParsedResult(newYql.ToString(), sqlParamsBuilder.ToSqlParams);
    }

    private static int SkipTerminals(string sql, char stopSymbol, int curToken)
    {
        while (++curToken < sql.Length)
        {
            if (sql[curToken] == '\\')
            {
                ++curToken;
                continue;
            }

            if (sql[curToken] == stopSymbol)
            {
                return ++curToken;
            }
        }

        return curToken;
    }

    // invariant sql[curToken] == '-'
    private static int ParseLineComment(string sql, int curToken)
    {
        if (curToken + 1 >= sql.Length || sql[curToken + 1] != '-')
        {
            return curToken + 1;
        }

        for (; curToken < sql.Length && sql[curToken] != '\r' && sql[curToken] != '\n'; curToken++)
        {
        }

        return curToken;
    }

    // invariant sql[curToken] == '/'
    private static int ParseBlockComment(string sql, int curToken)
    {
        if (curToken + 1 >= sql.Length || sql[curToken + 1] != '*')
        {
            return curToken + 1;
        }

        // /* /* */ */ nest, according to SQL spec
        var level = 1;
        for (curToken += 2; curToken < sql.Length; curToken++)
        {
            switch (sql[curToken - 1])
            {
                case '*':
                    if (sql[curToken] == '/')
                    {
                        --level;
                        ++curToken; // don't parse / in */* twice
                    }

                    break;
                case '/':
                    if (sql[curToken] == '*')
                    {
                        ++level;
                        ++curToken; // don't parse * in /*/ twice
                    }

                    break;
            }

            if (level == 0)
            {
                break;
            }
        }

        return curToken;
    }

    // invariant sql[curToken] == '@' && curToken + 1 < sql.Length && sql[curToken + 1] == '@'
    // https://ydb.tech/docs/en/yql/reference/syntax/lexer#multiline-string-literals
    private static int ParseMultilineStringLiterals(string sql, int curToken)
    {
        for (curToken += 2; curToken + 1 < sql.Length && (sql[curToken] != '@' || sql[curToken + 1] != '@'); curToken++)
        {
        }

        // sql[curToken] == '@' && sql[curToken + 1] == '@'
        return curToken + 2;
    }

    private static bool ParseInKeyWord(string sql, int keyWordStart) => sql.Length - keyWordStart >= 2 &&
                                                                        (sql[keyWordStart] | 0x20) == 'i' &&
                                                                        (sql[keyWordStart + 1] | 0x20) == 'n';


    private static int ParseInListParameters(
        string sql,
        int curToken,
        SqlParamsBuilder sqlParamsBuilder,
        StringBuilder yql
    )
    {
        var startToken = curToken;
        var listStartToken = -1;
        var findNameParams = new List<string>();
        var waitParam = false;

        while (curToken < sql.Length)
        {
            switch (sql[curToken])
            {
                case ',':
                    if (listStartToken < 0 || waitParam)
                    {
                        return startToken; // rollback parse IN LIST
                    }

                    waitParam = true;
                    curToken++;

                    break;
                case '@':
                    if (!waitParam || curToken + 1 < sql.Length && sql[curToken + 1] == '@')
                    {
                        return startToken;
                    }

                    var parsedParam = ParseNameParam(sql, curToken);

                    findNameParams.Add(parsedParam.Name);
                    curToken = parsedParam.NextToken;
                    waitParam = false;

                    break; // curToken
                case '(':
                    if (listStartToken >= 0)
                    {
                        return startToken; // rollback parse IN LIST
                    }

                    listStartToken = curToken;
                    waitParam = true;
                    curToken++;
                    break;
                case ')':
                    if (waitParam || findNameParams.Count == 0 || listStartToken < 0)
                    {
                        return startToken; // rollback parse IN LIST
                    }

                    yql.Append(listStartToken > startToken ? sql[startToken .. listStartToken] : ' ');
                    var paramListName = sqlParamsBuilder.AddListPrimitiveParams(findNameParams);
                    yql.Append(paramListName);

                    return curToken + 1;
                case '-':
                    curToken = ParseLineComment(sql, curToken);
                    break;
                case '/':
                    curToken = ParseBlockComment(sql, curToken);
                    break;
                default:
                    if (!char.IsWhiteSpace(sql[curToken]))
                    {
                        return startToken; // rollback parse IN LIST
                    }

                    curToken++;
                    break;
            }
        }

        return startToken;
    }

    // invariant sql[curToken] == '@'
    private static (string Name, int NextToken) ParseNameParam(string sql, int curToken)
    {
        var prevToken = ++curToken;

        for (;
             curToken < sql.Length && (char.IsLetterOrDigit(sql[curToken]) || sql[curToken] == '_');
             curToken++)
        {
        }

        if (curToken - prevToken == 0)
        {
            throw new YdbException($"Have empty name parameter, invalid SQL [position: {prevToken}]");
        }

        return ($"${sql[prevToken .. curToken]}", curToken);
    }

    private class SqlParamsBuilder
    {
        private readonly HashSet<string> _foundParamNames = new();
        private readonly List<ISqlParam> _sqlParams = new();

        private int _globalNumberListPrimitiveParam;

        internal void AddPrimitiveParam(string paramName, bool isNative)
        {
            if (_foundParamNames.Contains(paramName))
            {
                return;
            }

            _sqlParams.Add(new PrimitiveParam(paramName, isNative));
            _foundParamNames.Add(paramName);
        }

        internal string AddListPrimitiveParams(IReadOnlyList<string> paramNames)
        {
            var listPrimitiveParam = new ListPrimitiveParam(paramNames, ++_globalNumberListPrimitiveParam);

            _sqlParams.Add(listPrimitiveParam);

            return listPrimitiveParam.Name;
        }

        internal IReadOnlyList<ISqlParam> ToSqlParams => _sqlParams;
    }
}

internal record ParsedResult(string ParsedSql, IReadOnlyList<ISqlParam> SqlParams);
