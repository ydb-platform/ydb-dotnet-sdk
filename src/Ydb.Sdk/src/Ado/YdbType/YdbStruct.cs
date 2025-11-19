using System.Collections;
using Ydb.Sdk.Ado.Internal;
using static Ydb.Sdk.Ado.Internal.YdbTypeExtensions;
using static Ydb.Sdk.Ado.Internal.YdbValueExtensions;

namespace Ydb.Sdk.Ado.YdbType;

public class YdbStruct : IEnumerable
{
    internal readonly StructType StructType = new();
    internal readonly Ydb.Value Value = new();

    public void Add(string name, object value, byte precision = 0, byte scale = 0)
    {
        if (value is decimal decimalValue)
        {
            StructType.Members.Add(new StructMember { Name = name, Type = DecimalType(precision, scale) });
            Value.Items.Add(decimalValue.PackDecimal(precision, scale));
            return;
        }

        var info = YdbPrimitiveTypeInfo.TryResolve(value.GetType());
        if (info != null)
        {
            StructType.Members.Add(new StructMember { Name = name, Type = info.YdbType });
            Value.Items.Add(info.Pack(value)!);
            return;
        }

        throw new ArgumentException($"Type '{value.GetType()}' is not supported in YdbStruct.");
    }

    public void Add(string name, object? value, YdbDbType ydbDbType, byte precision = 0, byte scale = 0)
    {
        if (ydbDbType == YdbDbType.Decimal)
        {
            var ydbType = DecimalType(precision, scale);
            if (value is null)
            {
                StructType.Members.Add(new StructMember { Name = name, Type = ydbType.OptionalType() });
                Value.Items.Add(YdbValueNull);
                return;
            }

            if (value is not decimal dec)
                throw new ArgumentException($"Value for decimal field '{name}' must be decimal.", nameof(value));

            StructType.Members.Add(new StructMember { Name = name, Type = ydbType });
            Value.Items.Add(dec.PackDecimal(precision, scale));
            return;
        }

        var info = ydbDbType.PrimitiveTypeInfo() ??
                   throw new ArgumentException($"Unsupported YdbDbType '{ydbDbType}' in YdbStruct.", nameof(ydbDbType));
        if (value is null)
        {
            StructType.Members.Add(new StructMember { Name = name, Type = info.OptionalYdbType });
            Value.Items.Add(YdbValueNull);
            return;
        }

        StructType.Members.Add(new StructMember { Name = name, Type = info.YdbType });
        Value.Items.Add(info.Pack(value) ??
                        throw new ArgumentException($"Packing failed for field '{name}'.", nameof(value)));
    }

    public void Add(YdbParameter ydbParameter)
    {
        var typedValue = ydbParameter.TypedValue;

        StructType.Members.Add(new StructMember { Name = ydbParameter.ParameterName[1..], Type = typedValue.Type });
        Value.Items.Add(typedValue.Value);
    }

    public IEnumerator GetEnumerator() =>
        StructType.Members.Select((t, i) => (t.Name, Value.Items[i])).GetEnumerator();
}
