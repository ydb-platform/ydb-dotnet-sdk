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
        var paramNames = new List<string>();

        var prevToken = 0;

        for (var curToken = 0; curToken < sql.Length; curToken++)
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
                    if (curToken + 1 < sql.Length && sql[curToken + 1] == '-')
                    {
                        while (curToken + 1 < sql.Length)
                        {
                            curToken++;
                            if (sql[curToken] == '\r' || sql[curToken] == '\n')
                            {
                                break;
                            }
                        }
                    }

                    break;
                case '/':
                    if (curToken + 1 < sql.Length && sql[curToken + 1] == '*')
                    {
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
                    }

                    break;
                case '@':
                    if (curToken + 1 < sql.Length && sql[curToken + 1] == '@') // $text = @@ a b c @ @@ 
                    {
                        for (curToken+=2; curToken + 1 < sql.Length; curToken++)
                        {
                            if (sql[curToken] == '@' && sql[curToken + 1] == '@')
                            {
                                curToken++;
                                break;
                            }
                        }
                        
                        break;
                    }

                    // Parse params
                    newYql.Append(sql[prevToken .. curToken]);
                    prevToken = ++curToken;
                    
                    for (;
                         curToken < sql.Length && (char.IsLetterOrDigit(sql[curToken]) || sql[curToken] == '_');
                         curToken++)
                    {
                    }

                    if (curToken - prevToken == 0)
                    {
                        throw new YdbException($"Have empty name parameter, invalid SQL [position: {prevToken}]");
                    }

                    var originalParamName = $"${sql[prevToken .. curToken]}";

                    paramNames.Add(originalParamName);
                    newYql.Append(originalParamName);
                    prevToken = curToken;

                    break;
            }
        }

        newYql.Append(sql.AsSpan(prevToken, sql.Length - prevToken));

        return CacheQueries[sql] = new ParsedResult(newYql.ToString(), paramNames.ToArray());
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
                return curToken;
            }
        }

        return sql.Length;
    }
}

internal record ParsedResult(string ParsedSql, IReadOnlyList<string> ParamNames);
