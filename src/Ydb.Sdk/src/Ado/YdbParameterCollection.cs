using System.Collections;
using System.Data;
using System.Data.Common;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado;

public sealed class YdbParameterCollection : DbParameterCollection, IList<YdbParameter>
{
    private readonly List<YdbParameter> _parameters = new(5);

    internal YdbParameterCollection()
    {
    }

    internal Dictionary<string, YdbValue> YdbParameters =>
        _parameters.ToDictionary(p => p.ParameterName, p => p.YdbValue);

    /// <summary>
    /// Adds a <see cref="YdbParameter"/> to the <see cref="YdbParameterCollection"/> given the specified parameter name and
    /// value.
    /// </summary>
    /// <param name="parameterName">The name of the <see cref="YdbParameter"/>.</param>
    /// <param name="value">The value of the <see cref="YdbParameter"/> to add to the collection.</param>
    public void AddWithValue(string parameterName, object value)
        => Add(new YdbParameter(parameterName, value));

    /// <summary>
    /// Adds a <see cref="YdbParameter"/> to the <see cref="YdbParameterCollection"/> given the specified parameter name,
    /// data type and value.
    /// </summary>
    /// <param name="parameterName">The name of the <see cref="YdbParameter"/>.</param>
    /// <param name="parameterType">One of the NpgsqlDbType values.</param>
    /// <param name="value">The value of the <see cref="YdbParameter"/> to add to the collection.</param>
    /// <returns>The parameter that was added.</returns>
    public void AddWithValue(string parameterName, DbType parameterType, object? value = null)
        => Add(new YdbParameter(parameterName, parameterType) { Value = value });

    /// <inheritdoc />
    public override int Add(object value)
    {
        _parameters.Add(Cast(value));

        return _parameters.Count - 1;
    }

    /// <summary>
    /// Adds the specified <see cref="YdbParameter"/> object to the <see cref="YdbParameterCollection"/>.
    /// </summary>
    /// <param name="item">The <see cref="YdbParameter"/> to add to the collection.</param>
    public void Add(YdbParameter item)
    {
        _parameters.Add(item);
    }

    /// <summary>
    /// Removes all items from the collection.
    /// </summary>
    public override void Clear()
    {
        _parameters.Clear();
    }

    /// <summary>
    /// Report whether the specified parameter is present in the collection.
    /// </summary>
    /// <param name="item">Parameter to find.</param>
    /// <returns>True if the parameter was found, otherwise false.</returns>
    public bool Contains(YdbParameter item)
    {
        return _parameters.Contains(item);
    }

    /// <inheritdoc />
    public override bool Contains(object value)
    {
        return _parameters.Contains(value);
    }

    /// <inheritdoc />
    public void CopyTo(YdbParameter[] array, int arrayIndex)
    {
        _parameters.CopyTo(array, arrayIndex);
    }

    /// <summary>
    /// Remove the specified parameter from the collection.
    /// </summary>
    /// <param name="item">Parameter to remove.</param>
    /// <returns>True if the parameter was found and removed, otherwise false.</returns>
    public bool Remove(YdbParameter item)
    {
        return _parameters.Remove(item);
    }
    
    /// <inheritdoc />
    public override int IndexOf(object value)
    {
        return _parameters.IndexOf(Cast(value));
    }

    /// <inheritdoc />
    public override void Insert(int index, object value)
    {
        _parameters[index] = Cast(value);
    }

    /// <summary>
    /// Removes the specified <see cref="YdbParameter"/> from the collection.
    /// </summary>
    /// <param name="value">The <see cref="YdbParameter"/> to remove from the collection.</param>
    public override void Remove(object value)
    {
        Remove(Cast(value));
    }

    /// <summary>
    /// Report the offset within the collection of the given parameter.
    /// </summary>
    /// <param name="item">Parameter to find.</param>
    /// <returns>Index of the parameter, or -1 if the parameter is not present.</returns>
    public int IndexOf(YdbParameter item)
    {
        return _parameters.IndexOf(item);
    }

    /// <inheritdoc />
    public void Insert(int index, YdbParameter item)
    {
        _parameters[index] = item;
    }

    /// <summary>
    /// Removes the specified <see cref="YdbParameter"/> from the collection using a specific index.
    /// </summary>
    /// <param name="index">The zero-based index of the parameter.</param>
    public override void RemoveAt(int index)
    {
        _parameters.RemoveAt(index);
    }

    /// <summary>
    /// Gets the <see cref="YdbParameter"/> at the specified index.
    /// </summary>
    /// <param name="index">The zero-based index of the <see cref="YdbParameter"/> to retrieve.</param>
    /// <value>The <see cref="YdbParameter"/> at the specified index.</value>
    public new YdbParameter this[int index]
    {
        get => _parameters[index];
        set => _parameters[index] = value;
    }

    /// <inheritdoc />
    public override void RemoveAt(string parameterName)
    {
        RemoveAt(IndexOf(parameterName));
    }

    /// <inheritdoc />
    protected override void SetParameter(int index, DbParameter value)
    {
        Insert(index, Cast(value));
    }

    /// <inheritdoc />
    protected override void SetParameter(string parameterName, DbParameter value)
    {
        var index = IndexOf(parameterName);

        if (index == -1)
        {
            throw new ArgumentException("Parameter not found");
        }

        _parameters[index] = Cast(value);
    }

    /// <summary>
    /// Gets the number of <see cref="YdbParameter"/> objects in the collection.
    /// </summary>
    /// <value>The number of <see cref="YdbParameter"/> objects in the collection.</value>
    public override int Count => _parameters.Count;

    /// <inheritdoc />
    public override object SyncRoot => ((ICollection)_parameters).SyncRoot;

    /// <inheritdoc />
    public override int IndexOf(string parameterName)
    {
        for (var i = 0; i < _parameters.Count; i++)
        {
            if (_parameters[i].ParameterName == parameterName)
            {
                return i;
            }
        }

        return -1;
    }

    /// <inheritdoc />
    public override bool Contains(string parameterName)
    {
        return IndexOf(parameterName) != -1;
    }

    /// <inheritdoc />
    public override void CopyTo(Array array, int index)
    {
        ((ICollection)_parameters).CopyTo(array, index);
    }

    /// <inheritdoc />
    IEnumerator<YdbParameter> IEnumerable<YdbParameter>.GetEnumerator()
    {
        return _parameters.GetEnumerator();
    }

    /// <inheritdoc />
    public override IEnumerator GetEnumerator()
    {
        return _parameters.GetEnumerator();
    }

    /// <inheritdoc />
    protected override YdbParameter GetParameter(int index)
    {
        return this[index];
    }

    /// <inheritdoc />
    protected override YdbParameter GetParameter(string parameterName)
    {
        var index = IndexOf(parameterName);

        if (index == -1)
            throw new ArgumentException("Parameter not found");

        return _parameters[index];
    }

    /// <inheritdoc />
    public override void AddRange(Array values)
    {
        foreach (var parameter in values)
            Add(Cast(parameter));
    }

    private static YdbParameter Cast(object? value)
    {
        if (value is YdbParameter ydbParameter)
        {
            return ydbParameter;
        }

        throw new InvalidCastException(
            $"The value \"{value}\" is not of type \"{nameof(YdbParameter)}\" and cannot be used in this parameter collection.");
    }
}
