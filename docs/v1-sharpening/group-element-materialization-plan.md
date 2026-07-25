# Implementation plan: GroupBy element materialization (EF-parity, currently fail-loud)

Feature: `GroupBy(o => o.CustomerId).Select(g => new { g.Key, Items = g.Select(x => x.Amount).ToList() })`
and `g.ToList()`, ordered/filtered forms. Currently throws `NormUnsupportedFeatureException`
(`QueryTranslator.GroupByProjection.cs`, the `arg is MethodCallExpression aggregateCall` block where
`TranslateGroupAggregateMethod` returns null). Turn the fail-loud into a real EF-style split query.

## Key insight — REUSE the existing split-query runtime
nORM already materializes a dependent collection via a second keyed query, groups in memory, attaches —
**including tenant + global-filter re-application on the second query**:
- `QueryPlan.cs:329` `record DependentQueryDefinition` (TargetMapping, ForeignKeyColumns, ParentKeyProperties,
  TargetCollectionProperty, CollectionElementType, FilterSql/Params, ElementProjection, Ordering/RowCap/Skip).
- `QueryExecutor.DependentQueries.cs` — `ExecuteDependentQueries[Async]` → `BuildDependentChildSql` (re-applies
  tenant + global filters at :298-308) → `AppendDependentQueryWhere` (`fk IN (@…)`) → `StitchChildrenToParents`
  (:491, groups by `ForeignKeyColumns[0].Getter(child)`) → `AssignCollectionToTarget` (:550, fills the
  materializer's pre-built empty list in place).
- Wired at `QueryTranslator.PlanGeneration.cs:581-585` (gated on `_detectedCollections.Count > 0`); consumed at
  `QueryExecutor.cs:379/544`; excluded from the simple fast path at `NormQueryProvider.Execution.cs:344`.

So the ONLY new code is: a shape predicate, a materializer skip+empty-list, a detect-instead-of-throw, and a
`DependentQueryDefinition` builder for the self-scan (no relation mapping).

## Field mapping (group element -> DependentQueryDefinition)
- `TargetMapping` = `_mapping` (self-scan of the grouped table).
- `ForeignKeyColumns` = `[ column of the group-key MemberExpression ]` via `_mapping.TryGetColumnForMemberAccess`.
- `ParentKeyProperties` = `[ anon "Key" member PropertyInfo ]` (from `_projection.Body` NewExpression.Members).
- `TargetCollectionProperty` = the anon `Items` member PropertyInfo (read-only anon → filled in place).
- `CollectionElementType` = element-projection return type (or the entity type for `g.ToList()`).
- `ElementProjection` = the inner `g.Select(x => …)` lambda (or null for `g.ToList()`).
- `FilterSql`/`Params` = rendered source WHEREs from `_groupOrderedFirstSourceWheres` (+ any `g.Where`) against
  `_mapping.EscTable` via `TranslateAgainstSubAlias` (fold-no-cache; empty params). **Security/correctness MUST.**
- `OrderingSql`/`RowCap`/`RowSkip` = from `g.OrderBy/ThenBy/Take/Skip`; the existing ordered path emits
  `ROW_NUMBER() OVER (PARTITION BY <fk> ORDER BY …)` = per-group order.

## Steps (each independently testable; guard STAYS fail-loud for un-done shapes → no silent-wrong)
1. `IsGroupElementCollection(arg, out elementProjection, out ordering/filter)` next to
   `IsShapedOrBareNavigationCollection` (`MaterializerFactory.ProjectionColumns.cs:355`): peel
   `ToList/ToArray → Select → OrderBy/ThenBy → Take/Skip → Where`, require the bottom is a **parameter of type
   `IGrouping<,>`**. Unit-test on hand-built trees.
2. Materializer skip+empty-list: OR the predicate into `Core.cs:197` (`nonCollectionArgCount`),
   `ProjectionColumns.cs:97` (skip in `ExtractColumnsFromProjection`), `ConstructorProjections.cs:191`
   (empty list). Unit-test: builds `new { Key, Items=empty List<int> }`, no ordinal crash (fake reader w/ Key col).
3. Detect-instead-of-throw in `BuildGroupBySelectClause` (`QueryTranslator.GroupByProjection.cs:214-242`):
   on `IsGroupElementCollection`, emit NO column, record into a new `_detectedGroupElementCollections` field
   (declare in `QueryTranslator.cs`, reset in `Lifecycle.cs:130/195`). Keep the throw for genuinely-unsupported
   group methods. Test: query-1 SQL is `SELECT CustomerId AS "Key" FROM … GROUP BY CustomerId`.
4. `BuildGroupElementDependentQueries()` in new partial `QueryTranslator.GroupElementSplit.cs`: build the
   `DependentQueryDefinition` per field mapping above; **fail loud** for composite/computed/converter key,
   key-not-projected, `_groupOrderedFirstSourceWheres == null` (join/window/set-op source), element projection
   referencing outer key (reuse `IsSafeChildProjection`), computed/converter order keys (reuse
   `RenderOrderingKeys`).
5. Wire in `PlanGeneration.cs:581-585`: after the `_detectedCollections` block, add
   `if (_t._detectedGroupElementCollections.Count > 0) (dependentQueries ??= new()).AddRange(_t.BuildGroupElementDependentQueries());`.
   No executor change.
6. Flip `tests/GroupElementCollectionFailLoudTests.cs` `Assert.Throws` → correct materialization; keep
   `Supported_scalar_aggregates_still_work`; rename file/class.
7. New oracle tests vs LINQ-to-Objects: mixed `{Key, Count, Total, Items}`; ordered; filtered; `g.ToList()`;
   **outer-Where** (hard case 5); **tenant isolation** (hard case 4, model on `IncludeTenantIsolationTests`);
   fail-loud regressions for composite/computed key + key-not-projected. Both sync + async provider.

## Slice 1 (land first, atomically): `GroupBy(<simple mapped column>).Select(g => new { g.Key, [scalar
aggregates…], Items = g.Select(x => <scalar>).ToList() })` with outer-Where + tenant/global on query 2.
Keep fail-loud for: composite/computed keys, key-not-projected, `g.OrderBy/Where/Take` in the element
(slice 2), `g.ToList()` whole-entity (slice 2 — trivial, `ElementProjection=null`), join/window/set-op sources.

## Hard cases / risks (see steps): composite key nested-anon materializer path (Core.cs:175) — fail loud;
computed key has no Column — fail loud; key-not-projected — fail loud; **outer-Where MUST carry to query 2**
(the one real gap the nav path lacks — use `_groupOrderedFirstSourceWheres`); tenant/global on query 2 —
FREE via `BuildDependentChildSql:298-308`; closure-capturing element projection → `_closureFoldedIntoSql=true`.

## Critical files
`QueryTranslator.GroupByProjection.cs` · `MaterializerFactory.ProjectionColumns.cs` ·
`MaterializerFactory.ConstructorProjections.cs` · `MaterializerFactory.Core.cs` · new
`QueryTranslator.GroupElementSplit.cs` · `QueryTranslator.SplitQueries.cs` (reference) ·
`QueryExecutor.DependentQueries.cs` (reused unchanged) · `tests/GroupElementCollectionFailLoudTests.cs`.
