#if NETCOREAPP3_1
using System;
#endif
using System.Diagnostics.CodeAnalysis;

namespace Ydb.Sdk.Value
{
    public partial class YdbValue
    {
        public static explicit operator bool(YdbValue value)
        {
            return (bool)GetObject(value, typeof(bool));
        }
        public static explicit operator bool?(YdbValue value)
        {
            return (bool?)GetOptionalObject(value, typeof(bool));
        }
        public static explicit operator sbyte(YdbValue value)
        {
            return (sbyte)GetObject(value, typeof(sbyte));
        }

        public static explicit operator sbyte?(YdbValue value)
        {
            return (sbyte?)GetOptionalObject(value, typeof(sbyte));
        }

        public static explicit operator byte(YdbValue value)
        {
            return (byte)GetObject(value, typeof(byte));
        }

        public static explicit operator byte?(YdbValue value)
        {
            return (byte?)GetOptionalObject(value, typeof(byte));
        }

        public static explicit operator short(YdbValue value)
        {
            return (short)GetObject(value, typeof(short));
        }

        public static explicit operator short?(YdbValue value)
        {
            return (short?)GetOptionalObject(value, typeof(short));
        }

        public static explicit operator ushort(YdbValue value)
        {
            return (ushort)GetObject(value, typeof(ushort));
        }

        public static explicit operator ushort?(YdbValue value)
        {
            return (ushort?)GetOptionalObject(value, typeof(ushort));
        }

        public static explicit operator int(YdbValue value)
        {
            return (int)GetObject(value, typeof(int));
        }

        public static explicit operator int?(YdbValue value)
        {
            return (int?)GetOptionalObject(value, typeof(int));
        }

        public static explicit operator uint(YdbValue value)
        {
            return (uint)GetObject(value, typeof(uint));
        }

        public static explicit operator uint?(YdbValue value)
        {
            return (uint?)GetOptionalObject(value, typeof(uint));
        }

        public static explicit operator long(YdbValue value)
        {
            return (long)GetObject(value, typeof(long));
        }

        public static explicit operator long?(YdbValue value)
        {
            return (long?)GetOptionalObject(value, typeof(long));
        }

        public static explicit operator ulong(YdbValue value)
        {
            return (ulong)GetObject(value, typeof(ulong));
        }

        public static explicit operator ulong?(YdbValue value)
        {
            return (ulong?)GetOptionalObject(value, typeof(ulong));
        }

        public static explicit operator float(YdbValue value)
        {
            return (float)GetObject(value, typeof(float));
        }

        public static explicit operator float?(YdbValue value)
        {
            return (float?)GetOptionalObject(value, typeof(float));
        }

        public static explicit operator double(YdbValue value)
        {
            return (double)GetObject(value, typeof(double));
        }

        public static explicit operator double?(YdbValue value)
        {
            return (double?)GetOptionalObject(value, typeof(double));
        }

        public static explicit operator DateTime(YdbValue value)
        {
            return (DateTime)GetObject(value, typeof(DateTime));
        }

        public static explicit operator DateTime?(YdbValue value)
        {
            return (DateTime?)GetOptionalObject(value, typeof(DateTime));
        }

        public static explicit operator TimeSpan(YdbValue value)
        {
            return (TimeSpan)GetObject(value, typeof(TimeSpan));
        }

        public static explicit operator TimeSpan?(YdbValue value)
        {
            return (TimeSpan?)GetOptionalObject(value, typeof(TimeSpan));
        }

        public static explicit operator string?(YdbValue value)
        {
            return (string?)GetOptionalObject(value, typeof(string));
        }

        public static explicit operator byte[]?(YdbValue value)
        {
            return (byte[]?)GetOptionalObject(value, typeof(byte[]));
        }

        private static object GetObject(YdbValue value, System.Type targetType)
        {
            return GetObjectInternal(value.TypeId, value, targetType);
        }

        private static object? GetOptionalObject(YdbValue value, System.Type targetType)
        {
            return value.TypeId == YdbTypeId.OptionalType
                ? GetObjectInternal(GetYdbTypeId(value._protoType.OptionalType.Item), value.GetOptional(), targetType)
                : GetObject(value, targetType);
        }

        [return: NotNullIfNotNull("value")]
        private static object? GetObjectInternal(YdbTypeId typeId, YdbValue? value, System.Type targetType)
        {
            switch (typeId)
            {
                case YdbTypeId.Bool: return value?.GetBool();
                case YdbTypeId.Int8: return value?.GetInt8();
                case YdbTypeId.Uint8: return value?.GetUint8();
                case YdbTypeId.Int16: return value?.GetInt16();
                case YdbTypeId.Uint16: return value?.GetUint16();
                case YdbTypeId.Int32: return value?.GetInt32();
                case YdbTypeId.Uint32: return value?.GetUint32();
                case YdbTypeId.Int64: return value?.GetInt64();
                case YdbTypeId.Uint64: return value?.GetUint64();
                case YdbTypeId.Float: return value?.GetFloat();
                case YdbTypeId.Double: return value?.GetDouble();
                case YdbTypeId.Date: return value?.GetDate();
                case YdbTypeId.Datetime: return value?.GetDatetime();
                case YdbTypeId.Timestamp: return value?.GetTimestamp();
                case YdbTypeId.Interval: return value?.GetInterval();
                case YdbTypeId.String: return value?.GetString();
                case YdbTypeId.Utf8: return value?.GetUtf8();
                case YdbTypeId.Yson: return value?.GetYson();
                case YdbTypeId.Json: return value?.GetJson();
                case YdbTypeId.JsonDocument: return value?.GetJsonDocument();
                default:
                    throw new InvalidCastException($"Cannot cast YDB type {typeId} to {targetType.Name}.");

            }
        }
    }
}
