using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.YdbType;

namespace Ydb.Sdk.Value;

/// <summary>
/// A wrapper above the list of YDB values. For bulk operations, it is used as
/// <c>List&lt;Struct&lt;...&gt;&gt;</c> with the fields in the same order as <c>columns</c>.
/// </summary>
public sealed class YdbList
{
    private readonly IReadOnlyList<object> _items;

    public YdbList(IEnumerable<object> items)
    {
        _items = items as IReadOnlyList<object> ?? items.ToList();
    }

    internal TypedValue ToTypedValue()
    {
        var typed = new List<TypedValue>(_items.Count);
        foreach (var item in _items)
        {
            var tv = item switch
            {
                YdbValue yv => yv.GetProto(),
                YdbParameter p => p.TypedValue,
                _ => new YdbParameter { Value = item }.TypedValue
            };
            typed.Add(tv);
        }

        return typed.List();
    }
}
