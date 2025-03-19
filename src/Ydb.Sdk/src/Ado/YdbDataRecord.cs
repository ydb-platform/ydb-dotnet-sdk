using System.Data.Common;

namespace Ydb.Sdk.Ado;

public class YdbDataRecord : DbDataRecord
{
    private readonly YdbDataReader _ydbDataReader;

    internal YdbDataRecord(YdbDataReader ydbDataReader)
    {
        _ydbDataReader = ydbDataReader;
    }

    public override bool GetBoolean(int i) => _ydbDataReader.GetBoolean(i);

    public override byte GetByte(int i) => _ydbDataReader.GetByte(i);

    public override long GetBytes(int i, long dataIndex, byte[]? buffer, int bufferIndex, int length) =>
        _ydbDataReader.GetBytes(i, dataIndex, buffer, bufferIndex, length);

    public override char GetChar(int i) => _ydbDataReader.GetChar(i);

    public override long GetChars(int i, long dataIndex, char[]? buffer, int bufferIndex, int length) =>
        _ydbDataReader.GetChars(i, dataIndex, buffer, bufferIndex, length);

    public override string GetDataTypeName(int i) => _ydbDataReader.GetDataTypeName(i);

    public override DateTime GetDateTime(int i) => _ydbDataReader.GetDateTime(i);

    public override decimal GetDecimal(int i) => _ydbDataReader.GetDecimal(i);

    public override double GetDouble(int i) => _ydbDataReader.GetDouble(i);

    public override System.Type GetFieldType(int i) => _ydbDataReader.GetFieldType(i);

    public override float GetFloat(int i) => _ydbDataReader.GetFloat(i);

    public override Guid GetGuid(int i) => _ydbDataReader.GetGuid(i);

    public override short GetInt16(int i) => _ydbDataReader.GetInt16(i);

    public override int GetInt32(int i) => _ydbDataReader.GetInt32(i);

    public override long GetInt64(int i) => _ydbDataReader.GetInt64(i);

    public override string GetName(int i) => _ydbDataReader.GetName(i);

    public override int GetOrdinal(string name) => _ydbDataReader.GetOrdinal(name);

    public override string GetString(int i) => _ydbDataReader.GetString(i);

    public override object GetValue(int i) => _ydbDataReader.GetValue(i);

    public override int GetValues(object[] values) => _ydbDataReader.GetValues(values);

    public override bool IsDBNull(int i) => _ydbDataReader.IsDBNull(i);

    public override int FieldCount => _ydbDataReader.FieldCount;

    public override object this[int i] => _ydbDataReader[i];

    public override object this[string name] => _ydbDataReader[name];

    public byte[] GetBytes(int i) => _ydbDataReader.GetBytes(i);

    public sbyte GetSByte(int i) => _ydbDataReader.GetSByte(i);

    public ulong GetUint16(int i) => _ydbDataReader.GetUint16(i);

    public ulong GetUint32(int i) => _ydbDataReader.GetUint32(i);

    public ulong GetUint64(int i) => _ydbDataReader.GetUint64(i);

    public string GetJson(int i) => _ydbDataReader.GetJson(i);

    public string GetJsonDocument(int i) => _ydbDataReader.GetJsonDocument(i);

    public TimeSpan GetInterval(int i) => _ydbDataReader.GetInterval(i);
}
