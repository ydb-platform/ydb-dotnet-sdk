using System.Data;
using Xunit;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tests.Ado;

public class YdbParameterCollectionTests
{
    private readonly YdbParameterCollection _ydbParameterCollection;

    public YdbParameterCollectionTests()
    {
        _ydbParameterCollection = new YdbParameterCollection();

        _ydbParameterCollection.AddWithValue("$param", 1);
        _ydbParameterCollection.AddWithValue("$param", 1.0);
        _ydbParameterCollection.AddWithValue("$param", DbType.String, "text");
    }

    [Fact]
    public void Contains_WhenPresetCollection_ReturnFindResult()
    {
        Assert.Contains(new YdbParameter("$param", 1), _ydbParameterCollection);
        Assert.Contains(new YdbParameter("$param", 1.0), _ydbParameterCollection);
        Assert.Contains(new YdbParameter("$param", DbType.String, "text"), _ydbParameterCollection);

        Assert.False(_ydbParameterCollection.Contains(123));

        Assert.DoesNotContain(new YdbParameter("$param", 123), _ydbParameterCollection);
    }

    [Fact]
    public void RemoveAndContains_WhenPresetCollection_RemovedItem()
    {
        Assert.True(_ydbParameterCollection.Remove(new YdbParameter("$param", 1)));
        Assert.False(_ydbParameterCollection.Remove(new YdbParameter("$other_param", 1)));

        Assert.Equal(
            "The value \"System.Object\" is not of type \"YdbParameter\" and cannot be used in this parameter collection.",
            Assert.Throws<InvalidCastException>(() => _ydbParameterCollection.Remove(new object())).Message);

        _ydbParameterCollection.RemoveAt(0);

        Assert.DoesNotContain(new YdbParameter("$param", 1.0), _ydbParameterCollection);

        Assert.True(_ydbParameterCollection.Contains("$param"));

        _ydbParameterCollection.RemoveAt("$param");
    }

    [Fact]
    public void Clear_WhenPresetCollection_EmptyCollection()
    {
        Assert.Equal(3, _ydbParameterCollection.Count);

        _ydbParameterCollection.Clear();

        Assert.Empty(_ydbParameterCollection);
    }

    [Fact]
    public void GetEnumerator_WhenPresetCollection_ForEachYdbParameters()
    {
        foreach (YdbParameter ydbParam in _ydbParameterCollection)
        {
            Assert.Equal("$param", ydbParam.ParameterName);
        }
    }

    [Fact]
    public void IndexOf_WhenPresetCollection_ReturnIndex()
    {
        Assert.Equal(1, _ydbParameterCollection.IndexOf(new YdbParameter("$param", 1.0)));

        _ydbParameterCollection[1] = new YdbParameter("$new_param", 123);

        Assert.Equal(1, _ydbParameterCollection.IndexOf("$new_param"));

        Assert.Equal(
            "The value \"System.Object\" is not of type \"YdbParameter\" and cannot be used in this parameter collection.",
            Assert.Throws<InvalidCastException>(() => _ydbParameterCollection.IndexOf(new object())).Message);
    }
}
