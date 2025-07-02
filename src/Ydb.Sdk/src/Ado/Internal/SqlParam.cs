using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.Internal;

internal interface ISqlParam
{
    bool IsNative { get; }

    string Name { get; }

    YdbValue YdbValueFetch(Dictionary<string, YdbValue> ydbParameters);
}

internal record PrimitiveParam(string Name, bool IsNative) : ISqlParam
{
    public YdbValue YdbValueFetch(Dictionary<string, YdbValue> ydbParameters) =>
        ydbParameters.Get(Name);
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

    public YdbValue YdbValueFetch(Dictionary<string, YdbValue> ydbParameters) => YdbValue
        .MakeList(_paramNames.Select(ydbParameters.Get).ToArray());
}

internal static class YdbParametersExtension
{
    internal static YdbValue Get(this Dictionary<string, YdbValue> ydbParameters, string name)
        => ydbParameters.TryGetValue(name, out var ydbValue)
            ? ydbValue
            : throw new YdbException($"Not found YDB parameter [name: {name}]");
}
