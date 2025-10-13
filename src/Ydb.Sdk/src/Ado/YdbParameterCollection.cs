using System.Collections;
using System.Data;
using System.Data.Common;

namespace Ydb.Sdk.Ado;

/// <summary>
/// Collects all parameters relevant to a <see cref="YdbCommand"/> as well as their respective mappings to DataSet columns.
/// This class cannot be inherited.
/// </summary>
/// <remarks>
/// YdbParameterCollection provides a strongly-typed collection of <see cref="YdbParameter"/> objects
/// used with YDB commands. It supports both indexed and named parameter access, and provides
/// methods for adding, removing, and managing parameters.
/// </remarks>
public sealed class YdbParameterCollection : DbParameterCollection, IList<YdbParameter>
{
    private readonly List<YdbParameter> _parameters = new(5);

    internal YdbParameterCollection()
    {
    }

    internal Dictionary<string, TypedValue> YdbParameters =>
        _parameters.ToDictionary(p => p.ParameterName, p => p.TypedValue);

    /// <summary>
    /// Adds a <see cref="YdbParameter"/> to the <see cref="YdbParameterCollection"/> given the specified parameter name and
    /// value.
    /// </summary>
    /// <param name="parameterName">The name of the <see cref="YdbParameter"/>.</param>
    /// <param name="value">The value of the <see cref="YdbParameter"/> to add to the collection.</param>
    public void AddWithValue(string parameterName, object value) => Add(new YdbParameter(parameterName, value));

    /// <summary>
    /// Adds a <see cref="YdbParameter"/> to the <see cref="YdbParameterCollection"/> given the specified parameter name,
    /// data type and value.
    /// </summary>
    /// <param name="parameterName">The name of the <see cref="YdbParameter"/>.</param>
    /// <param name="parameterType">One of the <see cref="DbType"/> values.</param>
    /// <param name="value">The value of the <see cref="YdbParameter"/> to add to the collection.</param>
    /// <returns>The parameter that was added.</returns>
    public void AddWithValue(string parameterName, DbType parameterType, object? value = null) =>
        Add(new YdbParameter(parameterName, parameterType) { Value = value });

    /// <summary>
    /// Adds a <see cref="YdbParameter"/> to the collection.
    /// </summary>
    /// <param name="item">The <see cref="YdbParameter"/> to add to the collection.</param>
    void ICollection<YdbParameter>.Add(YdbParameter item) => Add(item);

    /// <summary>
    /// Adds a parameter to the collection.
    /// </summary>
    /// <param name="value">The parameter to add to the collection.</param>
    /// <returns>The index of the added parameter.</returns>
    /// <exception cref="InvalidCastException">Thrown when the value is not of type <see cref="YdbParameter"/>.</exception>
    public override int Add(object value) => Add(Cast(value));

    /// <summary>
    /// Adds the specified <see cref="YdbParameter"/> object to the <see cref="YdbParameterCollection"/>.
    /// </summary>
    /// <param name="item">The <see cref="YdbParameter"/> to add to the collection.</param>
    public int Add(YdbParameter item)
    {
        _parameters.Add(item);

        return _parameters.Count - 1;
    }

    /// <summary>
    /// Removes all items from the collection.
    /// </summary>
    public override void Clear() => _parameters.Clear();

    /// <summary>
    /// Report whether the specified parameter is present in the collection.
    /// </summary>
    /// <param name="item">Parameter to find.</param>
    /// <returns>True if the parameter was found, otherwise false.</returns>
    public bool Contains(YdbParameter item) => _parameters.Contains(item);

    /// <summary>
    /// Determines whether the collection contains a specific parameter.
    /// </summary>
    /// <param name="value">The parameter to locate in the collection.</param>
    /// <returns>true if the parameter is found in the collection; otherwise, false.</returns>
    public override bool Contains(object value) => _parameters.Contains(value);

    /// <summary>
    /// Copies the elements of the collection to an array, starting at a particular array index.
    /// </summary>
    /// <param name="array">The one-dimensional array that is the destination of the elements copied from the collection.</param>
    /// <param name="arrayIndex">The zero-based index in array at which copying begins.</param>
    public void CopyTo(YdbParameter[] array, int arrayIndex) => _parameters.CopyTo(array, arrayIndex);

    /// <summary>
    /// Remove the specified parameter from the collection.
    /// </summary>
    /// <param name="item">Parameter to remove.</param>
    /// <returns>True if the parameter was found and removed, otherwise false.</returns>
    public bool Remove(YdbParameter item) => _parameters.Remove(item);

    /// <summary>
    /// Determines the index of a specific parameter in the collection.
    /// </summary>
    /// <param name="value">The parameter to locate in the collection.</param>
    /// <returns>The index of the parameter if found in the collection; otherwise, -1.</returns>
    public override int IndexOf(object? value) => _parameters.IndexOf(Cast(value));

    /// <summary>
    /// Inserts a parameter into the collection at the specified index.
    /// </summary>
    /// <param name="index">The zero-based index at which the parameter should be inserted.</param>
    /// <param name="value">The parameter to insert into the collection.</param>
    /// <exception cref="InvalidCastException">Thrown when the value is not of type <see cref="YdbParameter"/>.</exception>
    public override void Insert(int index, object value) => _parameters.Insert(index, Cast(value));

    /// <summary>
    /// Removes the specified <see cref="YdbParameter"/> from the collection.
    /// </summary>
    /// <param name="value">The <see cref="YdbParameter"/> to remove from the collection.</param>
    public override void Remove(object value) => Remove(Cast(value));

    /// <summary>
    /// Report the offset within the collection of the given parameter.
    /// </summary>
    /// <param name="item">Parameter to find.</param>
    /// <returns>Index of the parameter, or -1 if the parameter is not present.</returns>
    public int IndexOf(YdbParameter item) => _parameters.IndexOf(item);

    /// <summary>
    /// Inserts a <see cref="YdbParameter"/> into the collection at the specified index.
    /// </summary>
    /// <param name="index">The zero-based index at which the parameter should be inserted.</param>
    /// <param name="item">The <see cref="YdbParameter"/> to insert into the collection.</param>
    public void Insert(int index, YdbParameter item) => _parameters[index] = item;

    /// <summary>
    /// Removes the specified <see cref="YdbParameter"/> from the collection using a specific index.
    /// </summary>
    /// <param name="index">The zero-based index of the parameter.</param>
    public override void RemoveAt(int index) => _parameters.RemoveAt(index);

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

    /// <summary>
    /// Removes the <see cref="YdbParameter"/> with the specified name from the collection.
    /// </summary>
    /// <param name="parameterName">The name of the parameter to remove.</param>
    public override void RemoveAt(string parameterName) => RemoveAt(IndexOf(parameterName));

    /// <summary>
    /// Sets the parameter at the specified index.
    /// </summary>
    /// <param name="index">The zero-based index of the parameter to set.</param>
    /// <param name="value">The new parameter value.</param>
    protected override void SetParameter(int index, DbParameter value) => Insert(index, Cast(value));

    /// <summary>
    /// Sets the parameter with the specified name.
    /// </summary>
    /// <param name="parameterName">The name of the parameter to set.</param>
    /// <param name="value">The new parameter value.</param>
    /// <exception cref="ArgumentException">Thrown when the parameter with the specified name is not found.</exception>
    protected override void SetParameter(string parameterName, DbParameter value)
    {
        ArgumentNullException.ThrowIfNull(value);
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

    /// <summary>
    /// Gets an object that can be used to synchronize access to the collection.
    /// </summary>
    /// <value>An object that can be used to synchronize access to the collection.</value>
    public override object SyncRoot => ((ICollection)_parameters).SyncRoot;

    /// <summary>
    /// Gets the location of a <see cref="YdbParameter"/> in the collection.
    /// </summary>
    /// <param name="parameterName">The name of the parameter to find.</param>
    /// <returns>The zero-based location of the <see cref="YdbParameter"/> within the collection.</returns>
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

    /// <summary>
    /// Indicates whether a <see cref="YdbParameter"/> with the specified name exists in the collection.
    /// </summary>
    /// <param name="parameterName">The name of the parameter to find.</param>
    /// <returns>true if the collection contains the parameter; otherwise, false.</returns>
    public override bool Contains(string parameterName) => IndexOf(parameterName) != -1;

    /// <summary>
    /// Copies the elements of the collection to an array, starting at a particular array index.
    /// </summary>
    /// <param name="array">The one-dimensional array that is the destination of the elements copied from the collection.</param>
    /// <param name="index">The zero-based index in array at which copying begins.</param>
    public override void CopyTo(Array array, int index) => ((ICollection)_parameters).CopyTo(array, index);

    /// <summary>
    /// Returns an enumerator that iterates through the collection.
    /// </summary>
    /// <returns>An enumerator that can be used to iterate through the collection.</returns>
    IEnumerator<YdbParameter> IEnumerable<YdbParameter>.GetEnumerator() => _parameters.GetEnumerator();

    /// <summary>
    /// Returns an enumerator that iterates through the collection.
    /// </summary>
    /// <returns>An enumerator that can be used to iterate through the collection.</returns>
    public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();

    /// <summary>
    /// Gets the parameter at the specified index.
    /// </summary>
    /// <param name="index">The zero-based index of the parameter to retrieve.</param>
    /// <returns>The <see cref="YdbParameter"/> at the specified index.</returns>
    protected override YdbParameter GetParameter(int index) => this[index];

    /// <summary>
    /// Gets the parameter with the specified name.
    /// </summary>
    /// <param name="parameterName">The name of the parameter to retrieve.</param>
    /// <returns>The <see cref="YdbParameter"/> with the specified name.</returns>
    /// <exception cref="ArgumentException">Thrown when the parameter with the specified name is not found.</exception>
    protected override YdbParameter GetParameter(string parameterName)
    {
        var index = IndexOf(parameterName);

        if (index == -1)
            throw new ArgumentException("Parameter not found");

        return _parameters[index];
    }

    /// <summary>
    /// Adds an array of parameters to the collection.
    /// </summary>
    /// <param name="values">An array of parameters to add to the collection.</param>
    /// <exception cref="InvalidCastException">Thrown when any value in the array is not of type <see cref="YdbParameter"/>.</exception>
    public override void AddRange(Array values)
    {
        foreach (var parameter in values)
            Add(Cast(parameter));
    }

    private static YdbParameter Cast(object? value)
    {
        ArgumentNullException.ThrowIfNull(value);

        return value as YdbParameter ?? throw new InvalidCastException(
            $"The value \"{value}\" is not of type \"{nameof(YdbParameter)}\" and cannot be used in this parameter collection.");
    }
}
