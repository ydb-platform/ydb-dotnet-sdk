namespace Ydb.Sdk.Utils;

internal class EnumConverter
{
    public static TDestinationEnum Convert<TSourceEnum, TDestinationEnum>(TSourceEnum value)
        where TSourceEnum: Enum
        where TDestinationEnum: Enum
    { 
        return (TDestinationEnum)(object)System.Convert.ToInt32(value);
    }
}
