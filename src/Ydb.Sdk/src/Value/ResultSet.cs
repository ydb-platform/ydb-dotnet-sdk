using System.Collections;
using Google.Protobuf.Collections;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Value;

public static class ResultSetExtension
{
    internal static ResultSet FromProto(this Ydb.ResultSet resultSetProto)
    {
        return new ResultSet(resultSetProto);
    }
}

public class ResultSet
{
    public IReadOnlyDictionary<string, int> ColumnNameToOrdinal { get; }
    public IReadOnlyList<Column> Columns { get; }
    public IReadOnlyList<Row> Rows { get; }
    public bool Truncated { get; }

    internal ResultSet(Ydb.ResultSet resultSetProto)
    {
        Columns = resultSetProto.Columns.Select(c => new Column(c.Type, c.Name)).ToArray();

        ColumnNameToOrdinal = Columns
            .Select((c, idx) => (c.Name, Index: idx))
            .ToDictionary(t => t.Name, t => t.Index);

        Rows = new RowsList(resultSetProto.Rows, Columns, ColumnNameToOrdinal);
        Truncated = resultSetProto.Truncated;
    }

    public class Column
    {
        public string Name { get; }

        internal Type Type { get; }

        internal Column(Type type, string name)
        {
            Type = type;
            Name = name;
        }
    }

    private class RowsList : IReadOnlyList<Row>
    {
        private readonly RepeatedField<Ydb.Value> _rows;
        private readonly IReadOnlyList<Column> _columns;
        private readonly IReadOnlyDictionary<string, int> _columnsMap;

        internal RowsList(
            RepeatedField<Ydb.Value> rows,
            IReadOnlyList<Column> columns,
            IReadOnlyDictionary<string, int> columnsMap)
        {
            _rows = rows;
            _columns = columns;
            _columnsMap = columnsMap;
        }

        public int Count => _rows.Count;

        public Row this[int index] => new(_rows[index], _columns, _columnsMap);

        private IEnumerator<Row> GetRowsEnumerator()
        {
            return new Enumerator(_rows.GetEnumerator(), _columns, _columnsMap);
        }

        public IEnumerator<Row> GetEnumerator()
        {
            return GetRowsEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetRowsEnumerator();
        }

        private class Enumerator : IEnumerator<Row>
        {
            private readonly IEnumerator<Ydb.Value> _protoEnumerator;
            private readonly IReadOnlyList<Column> _columns;
            private readonly IReadOnlyDictionary<string, int> _columnsMap;

            internal Enumerator(
                IEnumerator<Ydb.Value> protoEnumerator,
                IReadOnlyList<Column> columns,
                IReadOnlyDictionary<string, int> columnsMap)
            {
                _protoEnumerator = protoEnumerator;
                _columns = columns;
                _columnsMap = columnsMap;
            }

            private Row CurrentRow => new(_protoEnumerator.Current, _columns, _columnsMap);

            public Row Current => CurrentRow;

            object IEnumerator.Current => CurrentRow;

            public void Dispose()
            {
                _protoEnumerator.Dispose();
            }

            public bool MoveNext()
            {
                return _protoEnumerator.MoveNext();
            }

            public void Reset()
            {
                _protoEnumerator.Reset();
            }
        }
    }

    public class Row
    {
        private readonly Ydb.Value _row;
        private readonly IReadOnlyList<Column> _columns;
        private readonly IReadOnlyDictionary<string, int> _columnsMap;

        internal Row(Ydb.Value row, IReadOnlyList<Column> columns, IReadOnlyDictionary<string, int> columnsMap)
        {
            _row = row;
            _columns = columns;
            _columnsMap = columnsMap;
        }

        public YdbValue this[int columnIndex]
        {
            get
            {
                if (columnIndex < 0 || columnIndex >= ColumnCount)
                {
                    ThrowHelper.ThrowIndexOutOfRangeException(ColumnCount);
                }

                return new YdbValue(_columns[columnIndex].Type, _row.Items[columnIndex]);
            }
        }

        public YdbValue this[string columnName] => this[_columnsMap[columnName]];

        private int ColumnCount => _columns.Count;
    }
}
