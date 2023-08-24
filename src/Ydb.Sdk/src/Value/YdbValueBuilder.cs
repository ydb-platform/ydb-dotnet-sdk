namespace Ydb.Sdk.Value
{
    public partial class YdbValue
    {
        public static YdbValue MakeBool(bool value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.Bool),
                new Ydb.Value
                {
                    BoolValue = value
                });
        }

        public static YdbValue MakeInt8(sbyte value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.Int8),
                new Ydb.Value
                {
                    Int32Value = value
                });
        }

        public static YdbValue MakeUint8(byte value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.Uint8),
                new Ydb.Value
                {
                    Uint32Value = value
                });
        }

        public static YdbValue MakeInt16(short value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.Int16),
                new Ydb.Value
                {
                    Int32Value = value
                });
        }

        public static YdbValue MakeUint16(ushort value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.Uint16),
                new Ydb.Value
                {
                    Uint32Value = value
                });
        }

        public static YdbValue MakeInt32(int value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.Int32),
                new Ydb.Value
                {
                    Int32Value = value
                });
        }

        public static YdbValue MakeUint32(uint value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.Uint32),
                new Ydb.Value
                {
                    Uint32Value = value
                });
        }

        public static YdbValue MakeInt64(long value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.Int64),
                new Ydb.Value
                {
                    Int64Value = value
                });
        }

        public static YdbValue MakeUint64(ulong value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.Uint64),
                new Ydb.Value
                {
                    Uint64Value = value
                });
        }

        public static YdbValue MakeFloat(float value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.Float),
                new Ydb.Value
                {
                    FloatValue = value
                });
        }

        public static YdbValue MakeDouble(double value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.Double),
                new Ydb.Value
                {
                    DoubleValue = value
                });
        }

        public static YdbValue MakeDate(DateTime value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.Date),
                new Ydb.Value
                {
                    Uint32Value = (uint)value.Subtract(DateTime.UnixEpoch).TotalDays
                });
        }

        public static YdbValue MakeDatetime(DateTime value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.Datetime),
                new Ydb.Value
                {
                    Uint32Value = (uint)value.Subtract(DateTime.UnixEpoch).TotalSeconds
                });
        }

        public static YdbValue MakeTimestamp(DateTime value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.Timestamp),
                new Ydb.Value
                {
                    Uint64Value = (ulong)(value.Subtract(DateTime.UnixEpoch).TotalMilliseconds * 1000)
                });
        }

        public static YdbValue MakeInterval(TimeSpan value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.Interval),
                new Ydb.Value
                {
                    Int64Value = (long)(value.TotalMilliseconds * 1000)
                });
        }

        public static YdbValue MakeString(byte[] value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.String),
                new Ydb.Value
                {
                    BytesValue = Google.Protobuf.ByteString.CopyFrom(value)
                });
        }

        public static YdbValue MakeUtf8(string value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.Utf8),
                new Ydb.Value
                {
                    TextValue = value
                });
        }

        public static YdbValue MakeYson(byte[] value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.Yson),
                new Ydb.Value
                {
                    BytesValue = Google.Protobuf.ByteString.CopyFrom(value)
                });
        }

        public static YdbValue MakeJson(string value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.Json),
                new Ydb.Value
                {
                    TextValue = value
                });
        }

        public static YdbValue MakeJsonDocument(string value)
        {
            return new YdbValue(
                MakePrimitiveType(Type.Types.PrimitiveTypeId.JsonDocument),
                new Ydb.Value
                {
                    TextValue = value
                });
        }

        private static byte GetDecimalScale(decimal value)
        {
            var bits = decimal.GetBits(value);
            var flags = bits[3];
            var scale = (byte)((flags >> 16) & 0x7F);
            return scale;
        }

        private static uint GetDecimalPrecision(decimal value)
        {
            var bits = decimal.GetBits(value);
            value = new decimal(lo: bits[0], mid: bits[1], hi: bits[2], isNegative: false, scale: 0);

            var precision = 0u;
            while (value != decimal.Zero)
            {
                value = Math.Round(value / 10);
                precision++;
            }

            return precision;
        }

        private static Ydb.Value MakeDecimalValue(decimal value)
        {
            var bits = decimal.GetBits(value);

            var low64 = ((ulong)(uint)bits[1] << 32) + (uint)bits[0];
            var high64 = (ulong)(uint)bits[2];

            unchecked
            {
                if (value < 0)
                {
                    low64 = ~low64;
                    high64 = ~high64;

                    if (low64 == (ulong)-1L)
                    {
                        high64 += 1;
                    }

                    low64 += 1;
                }
            }

            return new Ydb.Value
            {
                Low128 = low64,
                High128 = high64
            };
        }

        public static YdbValue MakeDecimalWithPrecision(decimal value, uint? precision = null, uint? scale = null)
        {
            var valueScale = GetDecimalScale(value);
            var valuePrecision = GetDecimalPrecision(value);
            scale ??= GetDecimalScale(value);
            precision ??= valuePrecision;
            
            if ((int)valuePrecision - valueScale > (int)precision - scale)
            {
                throw new InvalidCastException(
                    $"Decimal with precision ({valuePrecision}, {valueScale}) can't fit into ({precision}, {scale})");
            }

            value *= 1.00000000000000000000000000000m; // 29 zeros, max supported by c# decimal
            value = Math.Round(value, (int)scale);

            var type = new Type
            {
                DecimalType = new DecimalType { Scale = (uint)scale, Precision = (uint)precision }
            };

            var ydbValue = MakeDecimalValue(value);

            return new YdbValue(type, ydbValue);
        }

        public static YdbValue MakeDecimal(decimal value)
        {
            return MakeDecimalWithPrecision(value, 22, 9);
        }

        // TODO: EmptyOptional with complex types
        public static YdbValue MakeEmptyOptional(YdbTypeId typeId)
        {
            if (IsPrimitiveTypeId(typeId))
            {
                return new YdbValue(
                    new Type { OptionalType = new OptionalType { Item = MakePrimitiveType(typeId) } },
                    new Ydb.Value { NullFlagValue = new Google.Protobuf.WellKnownTypes.NullValue() });
            }

            if (typeId == YdbTypeId.DecimalType)
            {
                return new YdbValue(
                    new Type
                    {
                        OptionalType = new OptionalType { Item = new Type { DecimalType = new DecimalType() } }
                    },
                    new Ydb.Value { NullFlagValue = new Google.Protobuf.WellKnownTypes.NullValue() }
                );
            }

            throw new ArgumentException($"This type is not supported: {typeId}", nameof(typeId));
        }

        public static YdbValue MakeOptional(YdbValue value)
        {
            return new YdbValue(
                new Ydb.Type { OptionalType = new OptionalType { Item = value._protoType } },
                value.TypeId != YdbTypeId.OptionalType
                    ? value._protoValue
                    : new Ydb.Value { NestedValue = value._protoValue });
        }

        // TODO: MakeEmptyList with complex types
        public static YdbValue MakeEmptyList(YdbTypeId typeId)
        {
            return new YdbValue(
                new Ydb.Type { ListType = new ListType { Item = MakePrimitiveType(typeId) } },
                new Ydb.Value());
        }

        // TODO: Check items type
        public static YdbValue MakeList(IReadOnlyList<YdbValue> values)
        {
            if (values.Count == 0)
            {
                throw new ArgumentOutOfRangeException("values");
            }

            var value = new Ydb.Value();
            value.Items.Add(values.Select(v => v._protoValue));

            return new YdbValue(
                new Ydb.Type { ListType = new ListType { Item = values[0]._protoType } },
                value);
        }

        public static YdbValue MakeTuple(IReadOnlyList<YdbValue> values)
        {
            var type = new Ydb.Type()
            {
                TupleType = new TupleType()
            };

            type.TupleType.Elements.Add(values.Select(v => v._protoType));

            var value = new Ydb.Value();
            value.Items.Add(values.Select(v => v._protoValue));

            return new YdbValue(
                type,
                value);
        }

        public static YdbValue MakeStruct(IReadOnlyDictionary<string, YdbValue> members)
        {
            var type = new Ydb.Type()
            {
                StructType = new StructType()
            };

            type.StructType.Members.Add(
                members.Select(m => new StructMember { Name = m.Key, Type = m.Value._protoType }));

            var value = new Ydb.Value();
            value.Items.Add(members.Select(m => m.Value._protoValue));

            return new YdbValue(
                type,
                value);
        }

        private static Ydb.Type MakePrimitiveType(Type.Types.PrimitiveTypeId primitiveTypeId)
        {
            return new Ydb.Type { TypeId = primitiveTypeId };
        }

        private static Ydb.Type MakePrimitiveType(YdbTypeId typeId)
        {
            EnsurePrimitiveTypeId(typeId);
            return new Ydb.Type { TypeId = (Type.Types.PrimitiveTypeId)typeId };
        }

        private static bool IsPrimitiveTypeId(YdbTypeId typeId)
        {
            return (uint)typeId < YdbTypeIdRanges.ComplexTypesFirst;
        }

        private static void EnsurePrimitiveTypeId(YdbTypeId typeId)
        {
            if (!IsPrimitiveTypeId(typeId))
            {
                throw new ArgumentException($"Complex types aren't supported in current method: {typeId}", "typeId");
            }
        }


        private static YdbValue MakeOptionalOf<T>(T? value, YdbTypeId type, Func<T, YdbValue> func) where T : struct
        {
            if (value is null)
            {
                return MakeEmptyOptional(type);
            }
            else
            {
                return MakeOptional(func((T)value));
            }
        }

        public static YdbValue MakeOptionalBool(bool? value)
        {
            return MakeOptionalOf(value, YdbTypeId.Bool, MakeBool);
        }

        public static YdbValue MakeOptionalInt8(sbyte? value)
        {
            return MakeOptionalOf(value, YdbTypeId.Int8, MakeInt8);
        }

        public static YdbValue MakeOptionalUint8(byte? value)
        {
            return MakeOptionalOf(value, YdbTypeId.Uint8, MakeUint8);
        }

        public static YdbValue MakeOptionalInt16(short? value)
        {
            return MakeOptionalOf(value, YdbTypeId.Int16, MakeInt16);
        }

        public static YdbValue MakeOptionalUint16(ushort? value)
        {
            return MakeOptionalOf(value, YdbTypeId.Uint16, MakeUint16);
        }

        public static YdbValue MakeOptionalInt32(int? value)
        {
            return MakeOptionalOf(value, YdbTypeId.Int32, MakeInt32);
        }

        public static YdbValue MakeOptionalUint32(uint? value)
        {
            return MakeOptionalOf(value, YdbTypeId.Uint32, MakeUint32);
        }

        public static YdbValue MakeOptionalInt64(long? value)
        {
            return MakeOptionalOf(value, YdbTypeId.Int64, MakeInt64);
        }

        public static YdbValue MakeOptionalUint64(ulong? value)
        {
            return MakeOptionalOf(value, YdbTypeId.Uint64, MakeUint64);
        }

        public static YdbValue MakeOptionalFloat(float? value)
        {
            return MakeOptionalOf(value, YdbTypeId.Float, MakeFloat);
        }

        public static YdbValue MakeOptionalDouble(double? value)
        {
            return MakeOptionalOf(value, YdbTypeId.Double, MakeDouble);
        }

        public static YdbValue MakeOptionalDate(DateTime? value)
        {
            return MakeOptionalOf(value, YdbTypeId.Date, MakeDate);
        }

        public static YdbValue MakeOptionalDatetime(DateTime? value)
        {
            return MakeOptionalOf(value, YdbTypeId.Datetime, MakeDatetime);
        }

        public static YdbValue MakeOptionalTimestamp(DateTime? value)
        {
            return MakeOptionalOf(value, YdbTypeId.Timestamp, MakeTimestamp);
        }

        public static YdbValue MakeOptionalInterval(TimeSpan? value)
        {
            return MakeOptionalOf(value, YdbTypeId.Interval, MakeInterval);
        }

        public static YdbValue MakeOptionalString(byte[]? value)
        {
            if (value is null)
            {
                return MakeEmptyOptional(YdbTypeId.String);
            }
            else
            {
                return MakeOptional(MakeString(value));
            }
        }

        public static YdbValue MakeOptionalUtf8(string? value)
        {
            if (value is null)
            {
                return MakeEmptyOptional(YdbTypeId.Utf8);
            }
            else
            {
                return MakeOptional(MakeUtf8(value));
            }
        }

        public static YdbValue MakeOptionalYson(byte[]? value)
        {
            if (value is null)
            {
                return MakeEmptyOptional(YdbTypeId.Yson);
            }
            else
            {
                return MakeOptional(MakeYson(value));
            }
        }

        public static YdbValue MakeOptionalJson(string? value)
        {
            if (value is null)
            {
                return MakeEmptyOptional(YdbTypeId.Json);
            }
            else
            {
                return MakeOptional(MakeJson(value));
            }
        }

        public static YdbValue MakeOptionalJsonDocument(string? value)
        {
            if (value is null)
            {
                return MakeEmptyOptional(YdbTypeId.JsonDocument);
            }
            else
            {
                return MakeOptional(MakeJsonDocument(value));
            }
        }

        public static YdbValue MakeOptionalDecimal(decimal? value)
        {
            return MakeOptionalOf(value, YdbTypeId.DecimalType, MakeDecimal);
        }
    }
}