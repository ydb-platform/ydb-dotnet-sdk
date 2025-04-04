using Ydb.Scheme;

namespace Ydb.Sdk.Ado.Schema;

internal class YdbObject
{
    internal YdbObject(Entry.Types.Type type, string path)
    {
        Type = Enum.IsDefined(typeof(SchemeType), (int)type) ? (SchemeType)type : SchemeType.TypeUnspecified;
        Name = path;
        IsSystem = path.IsSystem();
    }

    public SchemeType Type { get; }

    public string Name { get; }

    public bool IsSystem { get; }
}
