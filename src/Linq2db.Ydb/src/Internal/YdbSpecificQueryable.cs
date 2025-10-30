using LinqToDB.DataProvider;
using LinqToDB.DataProvider.Ydb;

namespace LinqToDB.Internal.DataProvider.Ydb.Internal
{
	sealed class YdbSpecificQueryable<TSource>
		: DatabaseSpecificQueryable<TSource>,
			IYdbSpecificQueryable<TSource>
	{
		public YdbSpecificQueryable(IQueryable<TSource> queryable) : base(queryable) { }
	}
}
