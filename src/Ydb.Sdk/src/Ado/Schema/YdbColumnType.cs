using Ydb.Sdk.Ado.YdbType;
using static Ydb.Sdk.Ado.Internal.YdbTypeExtensions;

namespace Ydb.Sdk.Ado.Schema;

public sealed class YdbColumnType
{
    private readonly Type _protoType;

    public YdbDbType YdbDbType { get; }
    public byte Precision { get; }
    public byte Scale { get; }

    public YdbColumnType(YdbDbType ydbDbType, byte precision = 0, byte scale = 0)
    {
        YdbDbType = ydbDbType;
        Precision = precision;
        Scale = scale;

        _protoType = YdbDbType == YdbDbType.Decimal
            ? DecimalType(Precision, Scale)
            : YdbDbType.PrimitiveTypeInfo()?.YdbType
              ?? throw new ArgumentException("Unsupported `YdbDbType` for column type");
    }

    internal YdbColumnType(Type type)
    {
        _protoType = type;
        if (type.TypeCase == Type.TypeOneofCase.OptionalType)
            type = type.OptionalType.Item;

        if (type.TypeCase == Type.TypeOneofCase.DecimalType)
        {
            YdbDbType = YdbDbType.Decimal;
            Precision = checked((byte)type.DecimalType.Precision);
            Scale = checked((byte)type.DecimalType.Scale);
        }
        else
        {
            YdbDbType = type.TypeId.ToYdbDbType();
            Precision = 0;
            Scale = 0;
        }
    }

    internal Type ToProto() => _protoType;

    public override string ToString() => _protoType.YqlTableType();
}
