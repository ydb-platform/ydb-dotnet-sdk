using System.Data.Common;

namespace Ydb.Sdk.Ado;

public class YdbDataRecord : DbDataRecord
{
    private readonly YdbDataReader _ydbDataReader;

    internal YdbDataRecord(YdbDataReader ydbDataReader)
    {
        _ydbDataReader = ydbDataReader;
    }

    public override bool GetBoolean(int i)
    {
        return _ydbDataReader.GetBoolean(i);
    }

    public override byte GetByte(int i)
    {
        return _ydbDataReader.GetByte(i);
    }

    public override long GetBytes(int i, long dataIndex, byte[]? buffer, int bufferIndex, int length)
    {
        return _ydbDataReader.GetBytes(i, dataIndex, buffer, bufferIndex, length);
    }

    public override char GetChar(int i)
    {
        return _ydbDataReader.GetChar(i);
    }

    public override long GetChars(int i, long dataIndex, char[]? buffer, int bufferIndex, int length)
    {
        return _ydbDataReader.GetChars(i, dataIndex, buffer, bufferIndex, length);
    }

    public override string GetDataTypeName(int i)
    {
        return _ydbDataReader.GetDataTypeName(i);
    }

    public override DateTime GetDateTime(int i)
    {
        return _ydbDataReader.GetDateTime(i);
    }

    public override decimal GetDecimal(int i)
    {
        return _ydbDataReader.GetDecimal(i);
    }

    public override double GetDouble(int i)
    {
        return _ydbDataReader.GetDouble(i);
    }

    public override System.Type GetFieldType(int i)
    {
        return _ydbDataReader.GetFieldType(i);
    }

    public override float GetFloat(int i)
    {
        return _ydbDataReader.GetFloat(i);
    }

    public override Guid GetGuid(int i)
    {
        return _ydbDataReader.GetGuid(i);
    }

    public override short GetInt16(int i)
    {
        return _ydbDataReader.GetInt16(i);
    }

    public override int GetInt32(int i)
    {
        return _ydbDataReader.GetInt32(i);
    }

    public override long GetInt64(int i)
    {
        return _ydbDataReader.GetInt64(i);
    }

    public override string GetName(int i)
    {
        return _ydbDataReader.GetName(i);
    }

    public override int GetOrdinal(string name)
    {
        return _ydbDataReader.GetOrdinal(name);
    }

    public override string GetString(int i)
    {
        return _ydbDataReader.GetString(i);
    }

    public override object GetValue(int i)
    {
        return _ydbDataReader.GetValue(i);
    }

    public override int GetValues(object[] values)
    {
        return _ydbDataReader.GetValues(values);
    }

    public override bool IsDBNull(int i)
    {
        return _ydbDataReader.IsDBNull(i);
    }

    public override int FieldCount => _ydbDataReader.FieldCount;

    public override object this[int i] => _ydbDataReader[i];

    public override object this[string name] => _ydbDataReader[name];

    public byte[] GetBytes(int i)
    {
        return _ydbDataReader.GetBytes(i);
    }

    public sbyte GetSByte(int i)
    {
        return _ydbDataReader.GetSByte(i);
    }

    public ulong GetUint16(int i)
    {
        return _ydbDataReader.GetUint16(i);
    }

    public ulong GetUint32(int i)
    {
        return _ydbDataReader.GetUint32(i);
    }

    public ulong GetUint64(int i)
    {
        return _ydbDataReader.GetUint64(i);
    }

    public string GetJson(int i)
    {
        return _ydbDataReader.GetJson(i);
    }

    public string GetJsonDocument(int i)
    {
        return _ydbDataReader.GetJsonDocument(i);
    }

    public TimeSpan GetInterval(int i)
    {
        return _ydbDataReader.GetInterval(i);
    }
}
