using Xunit;
using Ydb.Sdk.Ado.Schema;
using Ydb.Sdk.Ado.YdbType;
using Ydb.Table;

namespace Ydb.Sdk.Ado.Tests;

public class YdbColumnDefaultsCoverageTests
{
    [Fact]
    public void Ctor_FromLiteralDefault_ParsesValue()
    {
        var columnMeta = new ColumnMeta
        {
            Name = "value",
            Type = new Type { TypeId = Type.Types.PrimitiveTypeId.Int32 },
            FromLiteral = new TypedValue
            {
                Type = new Type { TypeId = Type.Types.PrimitiveTypeId.Int32 },
                Value = new Ydb.Value { Int32Value = 42 }
            }
        };

        var description = new YdbColumnDescription(columnMeta);

        Assert.Equal(42, Assert.IsType<int>(description.DefaultValue));
        Assert.Null(description.SequenceDescription);
    }

    [Fact]
    public void Ctor_FromDecimalLiteralDefault_ParsesDecimal()
    {
        var decimalTypedValue = new YdbParameter("$value", 1.25m).TypedValue;
        var columnMeta = new ColumnMeta
        {
            Name = "value",
            Type = decimalTypedValue.Type,
            FromLiteral = decimalTypedValue
        };

        var description = new YdbColumnDescription(columnMeta);

        Assert.Equal(1.25m, Assert.IsType<decimal>(description.DefaultValue));
    }

    [Fact]
    public void Ctor_FromSequenceDefault_ParsesSequence()
    {
        var columnMeta = new ColumnMeta
        {
            Name = "id",
            Type = new Type { TypeId = Type.Types.PrimitiveTypeId.Int64 },
            FromSequence = new SequenceDescription
            {
                Name = "seq_name",
                MinValue = 10,
                MaxValue = 100,
                StartValue = 11,
                Cache = 7,
                Increment = 3,
                Cycle = true
            }
        };

        var description = new YdbColumnDescription(columnMeta);

        Assert.Null(description.DefaultValue);
        Assert.NotNull(description.SequenceDescription);
        Assert.Equal("seq_name", description.SequenceDescription!.Name);
        Assert.Equal(10, description.SequenceDescription.MinValue);
        Assert.Equal(100, description.SequenceDescription.MaxValue);
        Assert.Equal(11, description.SequenceDescription.StartValue);
        Assert.Equal(7UL, description.SequenceDescription.Cache);
        Assert.Equal(3, description.SequenceDescription.Increment);
        Assert.True(description.SequenceDescription.Cycle);
    }

    [Fact]
    public void ToProto_WithoutDefault_KeepsDefaultValueUnset()
    {
        var description = new YdbColumnDescription("id", YdbDbType.Int64) { IsNullable = false };

        var proto = description.ToProto();

        Assert.Equal(ColumnMeta.DefaultValueOneofCase.None, proto.DefaultValueCase);
    }

    [Fact]
    public void ToProto_DefaultValueObject_UsesLiteralConversion()
    {
        var description = new YdbColumnDescription("id", YdbDbType.Int64)
        {
            IsNullable = false,
            DefaultValue = 5L
        };

        var proto = description.ToProto();

        Assert.Equal(ColumnMeta.DefaultValueOneofCase.FromLiteral, proto.DefaultValueCase);
        Assert.Equal(5L, proto.FromLiteral.Value.Int64Value);
    }

    [Fact]
    public void ToProto_SequenceDefault_UsesFromSequence()
    {
        var description = new YdbColumnDescription("id", YdbDbType.Int64)
        {
            IsNullable = false,
            SequenceDescription = new YdbSequenceDescription("seq_name")
            {
                MinValue = 10,
                MaxValue = 100,
                StartValue = 11,
                Cache = 7,
                Increment = 3,
                Cycle = true
            }
        };

        var proto = description.ToProto();

        Assert.Equal(ColumnMeta.DefaultValueOneofCase.FromSequence, proto.DefaultValueCase);
        Assert.Equal("seq_name", proto.FromSequence.Name);
        Assert.Equal(10, proto.FromSequence.MinValue);
        Assert.Equal(100, proto.FromSequence.MaxValue);
        Assert.Equal(11, proto.FromSequence.StartValue);
        Assert.Equal(7UL, proto.FromSequence.Cache);
        Assert.Equal(3, proto.FromSequence.Increment);
        Assert.True(proto.FromSequence.Cycle);
    }

    [Fact]
    public void ToProto_WhenLiteralAndSequenceAreSet_Throws()
    {
        var description = new YdbColumnDescription("id", YdbDbType.Int64)
        {
            IsNullable = false,
            DefaultValue = 1L,
            SequenceDescription = new YdbSequenceDescription("seq_name")
        };

        var exception = Assert.Throws<InvalidOperationException>(() => description.ToProto());
        Assert.Equal("Column default cannot contain both literal and sequence values.", exception.Message);
    }
}
