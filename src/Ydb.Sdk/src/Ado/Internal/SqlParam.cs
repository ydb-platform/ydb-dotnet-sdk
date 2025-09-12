namespace Ydb.Sdk.Ado.Internal;

internal interface ISqlParam
{
    bool IsNative { get; }

    string Name { get; }

    TypedValue YdbValueFetch(Dictionary<string, TypedValue> ydbParameters);
}

internal record PrimitiveParam(string Name, bool IsNative) : ISqlParam
{
    public TypedValue YdbValueFetch(Dictionary<string, TypedValue> ydbParameters) => ydbParameters.Get(Name);
}

internal class ListPrimitiveParam : ISqlParam
{
    private const string PrefixParamName = "$Gen_List_Primitive";

    private readonly IReadOnlyList<string> _paramNames;

    public ListPrimitiveParam(IReadOnlyList<string> paramNames, int globalNumber)
    {
        _paramNames = paramNames;
        Name = $"{PrefixParamName}_{globalNumber}";
    }

    public string Name { get; }

    public bool IsNative => false;

    public TypedValue YdbValueFetch(Dictionary<string, TypedValue> ydbParameters) =>
        _paramNames.Select(ydbParameters.Get).ToArray().List();
}

internal static class YdbParametersExtension
{
    internal static TypedValue Get(this Dictionary<string, TypedValue> ydbParameters, string name)
        => ydbParameters.TryGetValue(name, out var typedValue)
            ? typedValue
            : throw new YdbException($"Not found YDB parameter [name: {name}]");
}
