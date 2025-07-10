using System.Data;
using Xunit;

namespace Ydb.Sdk.Ado.Tests;

public class YdbParameterCollectionTests
{
    private readonly YdbParameterCollection _ydbParameterCollection;

    private readonly YdbParameter _parameter4 = new("$param", true);

    public YdbParameterCollectionTests()
    {
        _ydbParameterCollection = new YdbParameterCollection();

        _ydbParameterCollection.AddWithValue("$param1", 1);
        _ydbParameterCollection.AddWithValue("$param2", 1.0);
        _ydbParameterCollection.AddWithValue("$param3", DbType.String, "text");

        Assert.Equal(3, _ydbParameterCollection.Add(_parameter4));
    }

    [Fact]
    public void Contains_WhenPresetCollection_ReturnFindResult()
    {
        Assert.Contains(_parameter4, _ydbParameterCollection);
        Assert.False(_ydbParameterCollection.Contains(123));
        Assert.DoesNotContain(new YdbParameter("$param", 123), _ydbParameterCollection);
    }

    [Fact]
    public void RemoveAndContains_WhenPresetCollection_RemovedItem()
    {
        Assert.True(_ydbParameterCollection.Remove(_parameter4));
        Assert.False(_ydbParameterCollection.Remove(new YdbParameter("$other_param", 1)));

        Assert.Equal(
            "The value \"System.Object\" is not of type \"YdbParameter\" and cannot be used in this parameter collection.",
            Assert.Throws<InvalidCastException>(() => _ydbParameterCollection.Remove(new object())).Message);

        _ydbParameterCollection.RemoveAt(0);
        Assert.False(_ydbParameterCollection.Contains("$param1"));
        Assert.False(_ydbParameterCollection.Contains("$param"));

        _ydbParameterCollection.RemoveAt("$param2");
        Assert.False(_ydbParameterCollection.Contains("$param2"));

        Assert.Single(_ydbParameterCollection);
    }

    [Fact]
    public void Clear_WhenPresetCollection_EmptyCollection()
    {
        Assert.Equal(4, _ydbParameterCollection.Count);

        _ydbParameterCollection.Clear();

        Assert.Empty(_ydbParameterCollection);
    }

    [Fact]
    public void GetEnumerator_WhenPresetCollection_ForEachYdbParameters()
    {
        foreach (YdbParameter ydbParam in _ydbParameterCollection)
        {
            Assert.StartsWith("$param", ydbParam.ParameterName);
        }
    }

    [Fact]
    public void IndexOf_WhenPresetCollection_ReturnIndex()
    {
        Assert.Equal(1, _ydbParameterCollection.IndexOf("$param2"));
        Assert.Equal(3, _ydbParameterCollection.IndexOf(_parameter4));

        _ydbParameterCollection[2] = new YdbParameter("$new_param", 123);

        Assert.Equal(2, _ydbParameterCollection.IndexOf("$new_param"));

        Assert.Equal(
            "The value \"System.Object\" is not of type \"YdbParameter\" and cannot be used in this parameter collection.",
            Assert.Throws<InvalidCastException>(() => _ydbParameterCollection.IndexOf(new object())).Message);
    }
}
