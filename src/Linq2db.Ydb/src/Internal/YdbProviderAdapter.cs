using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Numerics;
using System.Reflection;
using LinqToDB.Common;
using LinqToDB.DataProvider;
using LinqToDB.Expressions;
using LinqToDB.Internal.Expressions.Types;

namespace LinqToDB.Internal.DataProvider.Ydb.Internal
{
	/*
	 * Misc notes:
	 * - supported default isolation levels: Unspecified/Serializable (same behavior) === TxMode.SerializableRw
	 * 
	 * Optional/future features:
	 * - TODO: add provider-specific retry policy to support YdbException.IsTransientWhenIdempotent
	 * - TODO: add support for BeginTransaction(TxMode mode)
	 */
	public sealed class YdbProviderAdapter : IDynamicProviderAdapter
	{
		public  const string AssemblyName         = "Ydb.Sdk";
		public  const string ClientNamespace      = "Ydb.Sdk.Ado";

		private const string ProtosAssemblyName   = "Ydb.Protos";

		// custom reader methods
		readonly Func<DbDataReader, int, byte[]> _getBytes;
		readonly Func<DbDataReader, int, sbyte> _getSByte;
		readonly Func<DbDataReader, int, ushort> _getUInt16;
		readonly Func<DbDataReader, int, uint> _getUInt32;
		readonly Func<DbDataReader, int, ulong> _getUInt64;
		readonly Func<DbDataReader, int, TimeSpan> _getInterval;
		readonly Func<DbDataReader, int, string> _getJson;
		readonly Func<DbDataReader, int, string> _getJsonDocument;

// --- 2) Публичные (internal) статические врапперы для использования в Set* вызовах ---
		internal static byte[] GetBytes(DbDataReader r, int i) => Instance._getBytes(r, i);
		internal static sbyte GetSByte(DbDataReader r, int i) => Instance._getSByte(r, i);
		internal static ushort GetUInt16(DbDataReader r, int i) => Instance._getUInt16(r, i);
		internal static uint GetUInt32(DbDataReader r, int i) => Instance._getUInt32(r, i);
		internal static ulong GetUInt64(DbDataReader r, int i) => Instance._getUInt64(r, i);
		internal static TimeSpan GetInterval(DbDataReader r, int i) => Instance._getInterval(r, i);
		internal static string GetJson(DbDataReader r, int i) => Instance._getJson(r, i);
		internal static string GetJsonDocument(DbDataReader r, int i) => Instance._getJsonDocument(r, i);

		private Func<DbDataReader, int, T> BuildReaderGetter<T>(string methodName)
		{
			var mi = DataReaderType.GetMethod(methodName, new[] { typeof(int) });
			if (mi == null)
			{
				return (r, i) => r.GetFieldValue<T>(i);
			}

			var pR = Expression.Parameter(typeof(DbDataReader), "r");
			var pI = Expression.Parameter(typeof(int), "i");
			var call = Expression.Call(Expression.Convert(pR, DataReaderType), mi, pI);

			Expression body = call;
			if (mi.ReturnType != typeof(T))
				body = Expression.Convert(call, typeof(T));

			return Expression.Lambda<Func<DbDataReader, int, T>>(body, pR, pI).Compile();
		}

		YdbProviderAdapter()
		{

			var assembly = Common.Tools.TryLoadAssembly(AssemblyName, null)
				?? throw new InvalidOperationException($"Cannot load assembly {AssemblyName}.");
			var protosAsembly = Common.Tools.TryLoadAssembly(ProtosAssemblyName, null)
				?? throw new InvalidOperationException($"Cannot load assembly {ProtosAssemblyName}.");

			ConnectionType  = assembly.GetType($"{ClientNamespace}.YdbConnection",  true)!;
			CommandType     = assembly.GetType($"{ClientNamespace}.YdbCommand",     true)!;
			ParameterType   = assembly.GetType($"{ClientNamespace}.YdbParameter",   true)!;
			DataReaderType  = assembly.GetType($"{ClientNamespace}.YdbDataReader",  true)!;
			TransactionType = assembly.GetType($"{ClientNamespace}.YdbTransaction", true)!;

			_getBytes        = BuildReaderGetter<byte[]>("GetBytes");
			_getSByte        = BuildReaderGetter<sbyte>("GetSByte");
			_getUInt16       = BuildReaderGetter<ushort>("GetUInt16");
			_getUInt32       = BuildReaderGetter<uint>("GetUInt32");
			_getUInt64       = BuildReaderGetter<ulong>("GetUInt64");
			_getInterval     = BuildReaderGetter<TimeSpan>("GetInterval");
			_getJson         = BuildReaderGetter<string>("GetJson");
			_getJsonDocument = BuildReaderGetter<string>("GetJsonDocument");
			
			var parameterType = assembly.GetType($"{ClientNamespace}.YdbParameter"           , true)!;
			var dbType        = assembly.GetType($"{ClientNamespace}.YdbType.YdbDbType"      , true)!;
			var bulkCopy      = assembly.GetType("Ydb.Sdk.Ado.BulkUpsert.IBulkUpsertImporter", true)!;

			var typeMapper = new TypeMapper();

			typeMapper.RegisterTypeWrapper<YdbConnection>(ConnectionType);
			typeMapper.RegisterTypeWrapper<IBulkUpsertImporter>(bulkCopy);
			typeMapper.RegisterTypeWrapper<YdbParameter>(parameterType);
			typeMapper.RegisterTypeWrapper<YdbDbType>(dbType);

			typeMapper.FinalizeMappings();

			_connectionFactory = typeMapper.BuildTypedFactory<string, YdbConnection, DbConnection>(connectionString => new YdbConnection(connectionString));
			var mapped = typeMapper.MapExpression<Task>(() => YdbConnection.ClearAllPools());
			ClearAllPools = typeMapper.BuildFunc<Task>(Expression.Lambda<Func<Task>>(mapped));
			ClearPool = typeMapper.BuildFunc<DbConnection, Task>(
				typeMapper.MapLambda<YdbConnection, Task>((YdbConnection connection) => YdbConnection.ClearPool(connection)));
			
			var paramMapper   = typeMapper.Type<YdbParameter>();
			var dbTypeBuilder = paramMapper.Member(p => p.YdbDbType);
			SetDbType         = dbTypeBuilder.BuildSetter<DbParameter>();
			GetDbType         = dbTypeBuilder.BuildGetter<DbParameter>();

			MakeDecimalFromString = (value, p, s) =>
			{
				return decimal.Parse(value, NumberStyles.Number, CultureInfo.InvariantCulture);
			};

			var pConnection = Expression.Parameter(typeof(DbConnection));
			var pName       = Expression.Parameter(typeof(string));
			var pColumns    = Expression.Parameter(typeof(IReadOnlyList<string>));
			var pToken      = Expression.Parameter(typeof(CancellationToken));

			BeginBulkCopy = Expression.Lambda<Func<DbConnection, string, IReadOnlyList<string>, CancellationToken, IBulkUpsertImporter>>(
				typeMapper.MapExpression((DbConnection conn, string name, IReadOnlyList<string> columns, CancellationToken cancellationToken) => typeMapper.Wrap<IBulkUpsertImporter>(((YdbConnection)(object)conn).BeginBulkUpsertImport(name, columns, cancellationToken)), pConnection, pName, pColumns, pToken),
				pConnection, pName, pColumns, pToken)
				.CompileExpression();
		}

		record struct DecimalValue(ulong Low, ulong High, uint Precision, uint Scale);

		private static DecimalValue MakeDecimalValue(string value, int precision, int scale)
		{
			var valuePrecision = value.Count(char.IsDigit);
			var dot = value.IndexOf('.');
			var valueScale = dot == -1 ? 0 : value.Length - dot - 1;

			if (valueScale < scale)
			{
				if (dot == -1)
					value += ".";

				value += new string('0', scale - valueScale);
				valuePrecision += scale - valueScale;
			}

#if SUPPORTS_INT128
			var raw128 = Int128.Parse(value.Replace(".", ""), CultureInfo.InvariantCulture);

			var low64 = (ulong)(raw128 & 0xFFFFFFFFFFFFFFFF);
			var high64 = (ulong)(raw128 >> 64);
#else
			var raw128 = BigInteger.Parse(value.Replace(".", ""), CultureInfo.InvariantCulture);
			var bytes = raw128.ToByteArray();
			var raw = new byte[16];

			if (raw128 < BigInteger.Zero && bytes.Length < raw.Length)
			{
				for (var i = bytes.Length; i < raw.Length; i++)
					raw[i] = 0xFF;
			}

			Array.Copy(bytes, raw, bytes.Length > 16 ? 16 : bytes.Length);
			var low64 = BitConverter.ToUInt64(raw, 0);
			var high64 = BitConverter.ToUInt64(raw, 8);
#endif

			return new DecimalValue(low64, high64, (uint)precision, (uint)scale);
		}

		static readonly Lazy<YdbProviderAdapter> _lazy    = new (() => new ());
		internal static YdbProviderAdapter Instance => _lazy.Value;

		#region IDynamicProviderAdapter

		public Type ConnectionType  { get; }
		public Type DataReaderType  { get; }
		public Type ParameterType   { get; }
		public Type CommandType     { get; }
		public Type TransactionType { get; }

		readonly Func<string, DbConnection> _connectionFactory;
		public DbConnection CreateConnection(string connectionString) => _connectionFactory(connectionString);

		#endregion

		public Func<Task>               ClearAllPools { get; }
		public Func<DbConnection, Task> ClearPool     { get; }

		public Action<DbParameter, YdbDbType> SetDbType { get; }
		public Func  <DbParameter, YdbDbType> GetDbType { get; }

		// missing parameter value factories
		public Func<byte[], object> MakeYson { get; }
		public object YsonNull { get; }

		internal Func<string, int, int, object> MakeDecimalFromString { get; }

		internal Func<DbConnection, string, IReadOnlyList<string>, CancellationToken, IBulkUpsertImporter> BeginBulkCopy { get; }

		#region wrappers
		[Wrapper]
		internal sealed class YdbConnection
		{
			public YdbConnection(string connectionString) => throw new NotImplementedException();

			public IBulkUpsertImporter BeginBulkUpsertImport(string name, IReadOnlyList<string> columns, CancellationToken cancellationToken) => throw new NotImplementedException();

			public static Task ClearAllPools() => throw new NotImplementedException();

			public static Task ClearPool(YdbConnection connection) => throw new NotImplementedException();
		}

		[Wrapper]
		internal sealed class IBulkUpsertImporter : TypeWrapper
		{
			[SuppressMessage("Style", "IDE0051:Remove unused private members", Justification = "Used from reflection")]
			private static LambdaExpression[] Wrappers { get; } =
{
				// [0]: AddRowAsync
				(Expression<Func<IBulkUpsertImporter, object?[], ValueTask>>)((this_, row) => this_.AddRowAsync(row)),
				// [1]: FlushAsync
				(Expression<Func<IBulkUpsertImporter, ValueTask>>)(this_ => this_.FlushAsync()),
			};

			public IBulkUpsertImporter(object instance, Delegate[] wrappers) : base(instance, wrappers)
			{
			}

			public ValueTask AddRowAsync(object?[] row) => ((Func<IBulkUpsertImporter, object?[], ValueTask>)CompiledWrappers[0])(this, row);

			public ValueTask FlushAsync() => ((Func<IBulkUpsertImporter, ValueTask>)CompiledWrappers[1])(this);
		}

		[Wrapper]
		internal sealed class YdbValue
		{
			public YdbValue(ProtoType type, ProtoValue value) => throw new NotImplementedException();

			public static YdbValue MakeYson        (byte[] value) => throw new NotImplementedException();
			public static YdbValue MakeOptionalYson(byte[]? value) => throw new NotImplementedException();
		}

		[Wrapper("Value")]
		internal sealed class ProtoValue
		{
			public ProtoValue() => throw new NotImplementedException();

			public ulong High128
			{
				get => throw new NotImplementedException();
				set => throw new NotImplementedException();
			}

			public ulong Low128
			{
				get => throw new NotImplementedException();
				set => throw new NotImplementedException();
			}
		}

		[Wrapper("Type")]
		internal sealed class ProtoType
		{
			public ProtoType() => throw new NotImplementedException();

			public DecimalType DecimalType
			{
				get => throw new NotImplementedException();
				set => throw new NotImplementedException();
			}
		}

		[Wrapper]
		internal sealed class DecimalType
		{
			public DecimalType() => throw new NotImplementedException();

			public uint Scale
			{
				get => throw new NotImplementedException();
				set => throw new NotImplementedException();
			}

			public uint Precision
			{
				get => throw new NotImplementedException();
				set => throw new NotImplementedException();
			}
		}

		[Wrapper]
		private sealed class YdbParameter
		{
			public YdbDbType YdbDbType { get; set; }
		}

		[Wrapper]
		public enum YdbDbType
		{
			Bool         = 1,
			Bytes        = 13,
			Date         = 18,
			DateTime     = 19,
			Decimal      = 12,
			Double       = 11,
			Float        = 10,
			Int16        = 3,
			Int32        = 4,
			Int64        = 5,
			Int8         = 2,
			Interval     = 21,
			Json         = 15,
			JsonDocument = 16,
			Text         = 14,
			Timestamp    = 20,
			UInt16       = 7,
			UInt32       = 8,
			UInt64       = 9,
			UInt8        = 6,
			Unspecified  = 0,
			Uuid         = 17,

			// missing simple types:
			// DyNumber
			// Yson

			// missing simple non-column types:
			// TzDate
			// TzDateTime
			// TzTimestamp
		}

		#endregion
	}
}	
