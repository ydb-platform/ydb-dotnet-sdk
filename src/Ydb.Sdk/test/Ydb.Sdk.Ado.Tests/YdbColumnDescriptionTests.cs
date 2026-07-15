using Xunit;
using Ydb.Sdk.Value;
using Ydb.Sdk.Ado.Schema;
using Ydb.Sdk.Ado.YdbType;
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

        Assert.Equal(3, Assert.IsType<int>(description.DefaultValue));
        Assert.Null(description.SequenceDescription);
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

        Assert.Equal("John Doe", Assert.IsType<string>(description.DefaultValue));
        Assert.Null(description.SequenceDescription);
    }

    [Fact]
    public void ToProto_DefaultValueObject_PacksFromLiteral()
    {
        var description = new YdbColumnDescription("Sum", YdbDbType.Int32)
        {
            DefaultValue = 3
        };

        var proto = description.ToProto();

        Assert.Equal(ColumnMeta.DefaultValueOneofCase.FromLiteral, proto.DefaultValueCase);
        Assert.Equal(3, proto.FromLiteral.Value.Int32Value);
    }

    [Fact]
    public void ToProto_DefaultValueYdbValue_PacksFromLiteral()
    {
        var defaultValue = YdbValue.MakeInt32(7);
        var description = new YdbColumnDescription("Sum", YdbDbType.Int32)
        {
            DefaultValue = defaultValue
        };

        var proto = description.ToProto();

        Assert.Equal(ColumnMeta.DefaultValueOneofCase.FromLiteral, proto.DefaultValueCase);
        Assert.Equal(7, proto.FromLiteral.Value.Int32Value);
    }

    [Fact]
    public void ToProto_DefaultValueSequence_PacksFromSequence()
    {
        var description = new YdbColumnDescription("Id", YdbDbType.Int64)
        {
            SequenceDescription = new YdbSequenceDescription("my_seq")
        };

        var proto = description.ToProto();

        Assert.Equal(ColumnMeta.DefaultValueOneofCase.FromSequence, proto.DefaultValueCase);
        Assert.Equal("my_seq", proto.FromSequence.Name);
    }

    [Fact]
    public void Ctor_FromSequenceDefault_SetsSequenceDescription()
    {
        var columnMeta = new ColumnMeta
        {
            Name = "Id",
            Type = new Type { TypeId = Type.Types.PrimitiveTypeId.Int64 },
            FromSequence = new SequenceDescription { Name = "my_seq" }
        };

        var description = new YdbColumnDescription(columnMeta);

        Assert.Null(description.DefaultValue);
        Assert.NotNull(description.SequenceDescription);
        Assert.Equal("my_seq", description.SequenceDescription!.Name);
    }
}
