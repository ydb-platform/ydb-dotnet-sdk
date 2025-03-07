namespace EfCore.Ydb.Utilities;

internal static class ArrayUtil
{
    internal static readonly bool[][] TrueArrays =
    [
        [],
        [true],
        [true, true],
        [true, true, true],
    ];

    internal static readonly bool[][] FalseArrays =
    [
        [],
        [false],
        [false, false],
        [false, false, false],
    ];
}
