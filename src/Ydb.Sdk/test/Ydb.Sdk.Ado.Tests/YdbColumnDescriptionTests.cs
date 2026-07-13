using Xunit;
using Ydb.Sdk.Ado.Schema;
using Ydb.Table;

namespace Ydb.Sdk.Ado.Tests;

public class YdbColumnDescriptionTests
{
    [Fact]
    public void Ctor_FromLiteralInt32Default_ParsesLiteralValue()
    {
        var columnMeta = new ColumnMeta
        {
            Name = "Sum",
            Type = new Type { TypeId = Type.Types.PrimitiveTypeId.Int32 },
            FromLiteral = new TypedValue
            {
                Type = new Type { TypeId = Type.Types.PrimitiveTypeId.Int32 },
                Value = new Ydb.Value { Int32Value = 3 }
            }
        };

        var description = new YdbColumnDescription(columnMeta);

        Assert.Equal("3", description.DefaultValueExpression);
    }

    [Fact]
    public void Ctor_FromLiteralUtf8Default_ParsesLiteralValue()
    {
        var columnMeta = new ColumnMeta
        {
            Name = "Name",
            Type = new Type { OptionalType = new OptionalType { Item = new Type { TypeId = Type.Types.PrimitiveTypeId.Utf8 } } },
            FromLiteral = new TypedValue
            {
                Type = new Type { TypeId = Type.Types.PrimitiveTypeId.Utf8 },
                Value = new Ydb.Value { TextValue = "John Doe" }
            }
        };

        var description = new YdbColumnDescription(columnMeta);

        Assert.Equal("John Doe", description.DefaultValueExpression);
    }
}
