using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query;

public class QueryBuilder
{
    public QueryBuilder(string queryString)
    {
        QueryString = queryString;
        throw new NotImplementedException();
    }

    public QueryBuilder WithParam(string paramName, YdbValue value)
    {
        if (!paramName.StartsWith("$"))
        {
            paramName = $"${paramName}";
        }

        throw new NotImplementedException();
    }

    internal string QueryString { get; }

    internal readonly Dictionary<string, YdbValue> Parameters = new();
}