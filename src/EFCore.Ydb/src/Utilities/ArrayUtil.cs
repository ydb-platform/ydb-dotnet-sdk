namespace EntityFrameworkCore.Ydb.Utilities;

internal static class ArrayUtil
{
    internal static readonly bool[][] FalseArrays =
    [
        [],
        [false],
        [false, false],
        [false, false, false]
    ];

    internal static readonly bool[][] TrueArrays =
    [
        [],
        [true],
        [true, true],
        [true, true, true]
    ];
}
