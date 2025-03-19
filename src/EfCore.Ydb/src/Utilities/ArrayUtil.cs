namespace EfCore.Ydb.Utilities;

internal static class ArrayUtil
{
    internal static readonly bool[][] FalseArrays =
    {
        new bool[] { },
        new bool[] { false },
        new bool[] { false, false },
        new bool[] { false, false, false }
    };
}
