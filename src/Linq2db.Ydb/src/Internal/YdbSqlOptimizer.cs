using LinqToDB.Internal.SqlProvider;
using LinqToDB.Internal.SqlQuery;
using LinqToDB.Mapping;

namespace LinqToDB.Internal.DataProvider.Ydb.Internal
{
	public class YdbSqlOptimizer : BasicSqlOptimizer
	{
		public YdbSqlOptimizer(SqlProviderFlags sqlProviderFlags)
			: base(sqlProviderFlags) { }

		public override SqlExpressionConvertVisitor CreateConvertVisitor(bool allowModify)
			=> new YdbSqlExpressionConvertVisitor(allowModify);

		public override SqlStatement TransformStatement(SqlStatement statement, DataOptions dataOptions, MappingSchema mappingSchema)
		{
			statement = base.TransformStatement(statement, dataOptions, mappingSchema);

			switch (statement.QueryType)
			{
				case QueryType.Delete:
					// disable table alias
					statement = GetAlternativeDelete((SqlDeleteStatement)statement);
					statement.SelectQuery!.From.Tables[0].Alias = "$";
					break;
				case QueryType.Update:
					// disable table alias
					statement.SelectQuery!.From.Tables[0].Alias = "$";
					break;
				case QueryType.Insert:
					statement = CorrectUpdateStatement((SqlInsertStatement)statement);
					break;
			}

			return statement;
		}

		private SqlStatement CorrectUpdateStatement(SqlInsertStatement statement)
		{
			if (statement.SelectQuery != null
				&& statement.SelectQuery.Select.Columns.Count == statement.Insert.Items.Count)
			{
				for (var i = 0; i < statement.Insert.Items.Count; i++)
				{
					statement.SelectQuery.Select.Columns[i].Alias = ((SqlField)statement.Insert.Items[i].Column).Name;
				}

				statement.SelectQuery.DoNotSetAliases = true;
			}

			return statement;
		}

		public override SqlStatement Finalize(MappingSchema mappingSchema, SqlStatement statement, DataOptions dataOptions)
		{
			statement = base.Finalize(mappingSchema, statement, dataOptions);

			if (MoveScalarSubQueriesToCte(statement))
				FinalizeCteCompat(statement);

			return statement;
		}

		// Todo remove and replace for FinalizeCte
		private void FinalizeCteCompat(SqlStatement statement)
		{
			if (statement is not SqlStatementWithQueryBase withStmt)
				return;

			// 1) Собираем зависимости CTE: для каждого встреченного SqlCteTable регистрируем его Cte и все его зависимости.
			var deps = new Dictionary<CteClause, HashSet<CteClause>>();

			void TouchCteRefs(IQueryElement root)
			{
				root.Visit<Dictionary<CteClause, HashSet<CteClause>>>(deps, (map, e) =>
				{
					if (e is SqlCteTable cteRef)
						RegisterDependencyCompat(cteRef.Cte, map);
				});
			}

			if (withStmt is SqlMergeStatement merge)
			{
				TouchCteRefs(merge.Target);
				TouchCteRefs(merge.Source);
			}
			else
			{
				TouchCteRefs(withStmt.SelectQuery);
			}

			// Если CTE не используются — очищаем WITH.
			if (deps.Count == 0)
			{
				withStmt.With = null;
				return;
			}

			// Если провайдер не поддерживает CTE — кидаем исключение.
			if (!this.SqlProviderFlags.IsCommonTableExpressionsSupported)
				throw new LinqToDBException("DataProvider do not supports Common Table Expressions.");

			// 2) Уточняем флаги/самозависимости.
			foreach (var kv in deps)
			{
				// если у CTE нет зависимостей — точно не рекурсивная
				if (kv.Value.Count == 0)
					kv.Key.IsRecursive = false;

				// удаляем самоссылки
				kv.Value.Remove(kv.Key);
			}

			// 3) Топологическая сортировка CTE по зависимостям.
			var sorted = TopoSortCompat(deps);

			// 4) Делаем имена уникальными и задаём пустые по необходимости.
			var used = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
			var seq = 1;
			foreach (var cte in sorted)
			{
				if (string.IsNullOrEmpty(cte.Name) || used.Contains(cte.Name))
				{
					string newName;
					do newName = $"CTE_{seq++}";
					while (used.Contains(newName));
					cte.Name = newName;
				}

				used.Add(cte.Name);
			}

			// 5) Записываем WITH
			withStmt.With = new SqlWithClause();
			withStmt.With.Clauses.AddRange(sorted);
		}

// Рекурсивно регистрирует зависимости для одного CTE
		private static void RegisterDependencyCompat(CteClause cte, Dictionary<CteClause, HashSet<CteClause>> map)
		{
			if (map.ContainsKey(cte))
				return;

			var dependsOn = new HashSet<CteClause>();

			cte.Body.Visit<HashSet<CteClause>>(dependsOn, (set, e) =>
			{
				if (e is SqlCteTable refTable)
					set.Add(refTable.Cte);
			});

			map.Add(cte, dependsOn);

			foreach (var d in dependsOn)
				RegisterDependencyCompat(d, map);
		}

// Простой DFS-топосорт без внешних утилит
		private static List<CteClause> TopoSortCompat(Dictionary<CteClause, HashSet<CteClause>> graph)
		{
			var result = new List<CteClause>(graph.Count);
			var state = new Dictionary<CteClause, int>(graph.Count); // 0=unvisited,1=visiting,2=visited

			void Dfs(CteClause node)
			{
				if (state.TryGetValue(node, out var s))
				{
					if (s == 2) return;
					if (s == 1) return; // цикл: оставим как есть, чтобы не зациклиться
				}

				state[node] = 1;
				if (graph.TryGetValue(node, out var children))
				{
					foreach (var child in children)
						Dfs(child);
				}

				state[node] = 2;
				result.Add(node);
			}

			foreach (var n in graph.Keys)
				Dfs(n);

			// порядок обратный завершению — уже подходит для WITH (зависимости идут раньше использующих)
			return result;
		}

		private bool MoveScalarSubQueriesToCte(SqlStatement statement)
		{
			if (statement is not SqlStatementWithQueryBase withStatement)
				return false;

			var cteCount = withStatement.With?.Clauses.Count ?? 0;

			if (statement.SelectQuery != null && statement.QueryType != QueryType.Merge)
				statement.SelectQuery = ConvertToCte(statement.SelectQuery, withStatement);

			if (statement is SqlInsertStatement insert)
				insert.Insert = ConvertToCte(insert.Insert, withStatement);

			return withStatement.With?.Clauses.Count > cteCount;

			static T ConvertToCte<T>(T statement, SqlStatementWithQueryBase withStatement)
				where T: class, IQueryElement
			{
				return statement.Convert(withStatement, static (visitor, elem) =>
				{
					if (elem is SelectQuery { Select.Columns: [var column] } subQuery
						&& !QueryHelper.IsDependsOnOuterSources(subQuery))
					{
						if (column.SystemType == null)
							throw new InvalidOperationException();

						if (visitor.Stack?.Count > 1
							// in column or predicate
							&& visitor.Stack[^2] is SqlSelectClause
								or ISqlPredicate
								or SqlExpressionBase
								or SqlSetExpression)
						{
							var cte = new CteClause(subQuery, column.SystemType, false, null);
							(visitor.Context.With ??= new()).Clauses.Add(cte);
							return new SqlCteTable(cte, column.SystemType);
						}
					}

					return elem;
				}, true);
			}
		}
	}
}
