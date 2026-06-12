using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Reflection;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using RenameColumnAttr = nORM.Mapping.RenameColumnAttribute;

namespace nORM.Migration
{
    /// <summary>
    /// Helper responsible for creating <see cref="SchemaSnapshot"/> instances by
    /// scanning assemblies for entity types and their mapping attributes.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("SchemaSnapshotBuilder scans assemblies via reflection; not NativeAOT-compatible.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("SchemaSnapshotBuilder reflects over entity types; trimming may remove the required members.")]
    public static class SchemaSnapshotBuilder
    {
        /// <summary>
        /// Builds a snapshot of the entity schema by inspecting the types in the provided assembly.
        /// Reflects only attributes and conventions; fluent configuration is not visible.
        /// Use <see cref="Build(DbContext)"/> to include fluent model overrides.
        /// </summary>
        /// <param name="assembly">The assembly containing the entity types to scan.</param>
        /// <returns>A snapshot describing the tables and columns inferred from the assembly.</returns>
        /// <remarks>
        /// <para>
        /// <b>Differences from <see cref="Build(DbContext)"/>:</b>
        /// </para>
        /// <list type="bullet">
        ///   <item>Uses reflection, <c>[Table]</c>/<c>[Column]</c>/<c>[Key]</c>/<c>[DatabaseGenerated]</c>
        ///         attributes, and Id/TypeNameId conventions to discover entities and their columns.
        ///         Fluent API overrides (e.g. <c>ToTable()</c>, <c>HasColumnName()</c>, <c>HasKey()</c>)
        ///         are <b>not</b> visible here.</item>
        ///   <item>Scans all non-abstract classes in the assembly that carry <c>[Table]</c> or a
        ///         <c>[Key]</c>-annotated property. The context-based overload only includes types
        ///         explicitly registered with the fluent model builder.</item>
        ///   <item>Does not include foreign key constraints (no fluent Relation metadata available).</item>
        ///   <item>May produce duplicate <c>TableSchema</c> entries if two entity types share the
        ///         same <c>[Table]</c> name - see the duplicate-name guard for details.</item>
        /// </list>
        /// </remarks>
        public static SchemaSnapshot Build(Assembly assembly)
        {
            ArgumentNullException.ThrowIfNull(assembly);

            var snapshot = new SchemaSnapshot();
            foreach (var type in GetEntityCandidates(assembly))
            {
                var tableAttr = type.GetCustomAttribute<TableAttribute>();

                var table = new TableSchema
                {
                    Name = GetTableName(type, tableAttr)
                };

                // Collect PK property names for the type using [Key] or convention.
                // At most ONE convention PK is chosen: "Id" is checked first, then "{TypeName}Id".
                var pkNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var p in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                {
                    if (p.GetCustomAttribute<KeyAttribute>() != null)
                        pkNames.Add(p.Name);
                }
                // Convention fallback: if no [Key] found, try "Id" first, then "{TypeName}Id".
                // Stop after finding the first match to avoid creating a spurious composite PK
                // when a type happens to have both (e.g. Order with both "Id" and "OrderId").
                if (pkNames.Count == 0)
                {
                    if (type.GetProperty("Id", BindingFlags.Public | BindingFlags.Instance) != null)
                        pkNames.Add("Id");
                    else
                    {
                        var conventionName = type.Name + "Id";
                        if (type.GetProperty(conventionName, BindingFlags.Public | BindingFlags.Instance) != null)
                            pkNames.Add(conventionName);
                    }
                }

                var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
                var indexAttributesByProperty = properties.ToDictionary(
                    static prop => prop,
                    static prop => prop.GetCustomAttributes<IndexAttribute>().ToArray());
                var indexColumnCounts = indexAttributesByProperty.Values
                    .SelectMany(static attrs => attrs.Select(static attr => attr.Name))
                    .GroupBy(static name => name, StringComparer.OrdinalIgnoreCase)
                    .ToDictionary(static group => group.Key, static group => group.Count(), StringComparer.OrdinalIgnoreCase);

                foreach (var prop in properties)
                {
                    // A property must be readable to be a column.
                    if (!prop.CanRead)
                        continue;
                    // Exclude properties with no setter at all - these are computed/expression-body
                    // properties (e.g. public string FullName => FirstName + " " + LastName) and have
                    // no backing database column. Note: init-only properties DO have a setter
                    // (IsInitOnly=true), so CanWrite returns true for them and they are included.
                    if (!prop.CanWrite)
                        continue;
                    if (prop.GetCustomAttribute<NotMappedAttribute>() != null)
                        continue;
                    // Exclude collection/enumerable navigation properties (e.g. List<Post>, ICollection<T>)
                    if (typeof(System.Collections.IEnumerable).IsAssignableFrom(prop.PropertyType)
                        && prop.PropertyType != typeof(string)
                        && prop.PropertyType != typeof(byte[]))
                        continue;
                    // Exclude reference navigation properties (class types that are not mappable scalars)
                    if (!IsMappableType(prop.PropertyType))
                        continue;

                    var colAttr = prop.GetCustomAttribute<ColumnAttribute>();
                    var clr = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                    // Skip open generic type parameters (e.g. T in MyEntity<T>) - FullName is null
                    // and the parameter name (e.g. "T") is ambiguous and not a valid SQL type.
                    if (clr.IsGenericTypeParameter)
                        continue;
                    var isPk = pkNames.Contains(prop.Name);
                    var dbGenAttr = prop.GetCustomAttribute<DatabaseGeneratedAttribute>();
                    var renameAttr = prop.GetCustomAttribute<RenameColumnAttr>();
                    var storeFacets = ParseStringBinaryFacets(colAttr?.TypeName, clr);
                    var (precision, scale) = TryParseDecimalPrecision(colAttr?.TypeName, clr);
                    var indexAttrs = indexAttributesByProperty[prop];
                    var firstIndexAttr = indexAttrs.FirstOrDefault();
                    var hasSingleColumnUniqueIndex = indexAttrs.Any(attr =>
                        attr.IsUnique &&
                        indexColumnCounts.TryGetValue(attr.Name, out var count) &&
                        count == 1);
                    var column = new ColumnSchema
                    {
                        Name = colAttr?.Name ?? prop.Name,
                        // Use ToString() as fallback: for constructed generic types it returns the
                        // full generic form (e.g. "System.Collections.Generic.List`1[System.Int32]")
                        // which is unambiguous, unlike Name which gives just "List`1".
                        ClrType = clr.FullName ?? clr.ToString(),
                        MaxLength = GetMaxLength(prop, clr, colAttr, storeFacets),
                        IsUnicode = storeFacets.IsUnicode,
                        IsFixedLength = storeFacets.IsFixedLength,
                        Precision = precision,
                        Scale = scale,
                        IsNullable = !prop.PropertyType.IsValueType || Nullable.GetUnderlyingType(prop.PropertyType) != null,
                        // Populate PK / index metadata from attributes or convention.
                        // IsUnique is only set for single-column PKs; composite PKs must NOT
                        // emit per-column UNIQUE constraints.
                        IsPrimaryKey = isPk,
                        IsUnique = (isPk && pkNames.Count == 1) || hasSingleColumnUniqueIndex,
                        IndexName = isPk ? "PK_" + table.Name : firstIndexAttr?.Name,
                        IndexOrder = firstIndexAttr is null ? null : firstIndexAttr.Order,
                        IsIdentity = dbGenAttr?.DatabaseGeneratedOption == DatabaseGeneratedOption.Identity,
                        ComputedColumnSql = dbGenAttr?.DatabaseGeneratedOption == DatabaseGeneratedOption.Computed
                            ? string.Empty
                            : null,
                        // [RenameColumn("OldName")] signals the differ that this is a rename, not drop+add.
                        PreviousName = renameAttr?.OldName,
                    };
                    foreach (var indexAttr in indexAttrs)
                    {
                        column.Indexes.Add(new ColumnIndexSchema
                        {
                            Name = indexAttr.Name,
                            IsUnique = indexAttr.IsUnique,
                            Order = indexAttr.Order,
                            IsDescending = indexAttr.IsDescending,
                            IsIncluded = indexAttr.IsIncluded,
                            NullsNotDistinct = indexAttr.NullsNotDistinct,
                            NullSortOrder = indexAttr.NullSortOrder,
                            FilterSql = indexAttr.FilterSql
                        });
                    }
                    table.Columns.Add(column);
                }

                snapshot.Tables.Add(table);
            }

            // Detect duplicate table names: two entity types sharing the same table name produce
            // two TableSchema entries with identical names, making migrations ambiguous.
            // Build(Assembly) is a best-effort reflection scan so we warn (not throw) and keep
            // the last entry for each name. Use Build(DbContext) for strict, fluent-config-aware
            // snapshot building where table-name uniqueness is enforced by the ORM itself.
            var seen = new Dictionary<string, TableSchema>(StringComparer.OrdinalIgnoreCase);
            var deduped = new List<TableSchema>(snapshot.Tables.Count);
            foreach (var tbl in snapshot.Tables)
            {
                if (seen.TryGetValue(tbl.Name, out _))
                    System.Diagnostics.Trace.TraceWarning(
                        $"[nORM SchemaSnapshot] Duplicate table name '{tbl.Name}' detected while scanning " +
                        $"assembly '{assembly.GetName().Name}'. Only the last definition will be used. " +
                        "Use [Table(\"UniqueName\")] to assign distinct names, or use Build(DbContext) " +
                        "for conflict-free snapshots.");
                seen[tbl.Name] = tbl;
                deduped.Add(tbl);
            }
            snapshot.Tables.Clear();
            foreach (var tbl in seen.Values) snapshot.Tables.Add(tbl);

            return snapshot;
        }

        /// <summary>
        /// Builds a snapshot using only the fluent-registered entity types from the provided
        /// <see cref="DbContext"/>. Unlike <see cref="Build(Assembly)"/>, this method does NOT
        /// scan the context's assembly; it operates exclusively on types that have been
        /// explicitly configured via the fluent API (e.g. <c>modelBuilder.Entity&lt;T&gt;()</c>).
        /// Fluent overrides such as <c>ToTable()</c>, <c>HasColumnName()</c>, and <c>HasKey()</c>
        /// are reflected in the returned snapshot.
        /// </summary>
        /// <param name="ctx">The context whose fluent model is used to build the snapshot.</param>
        /// <returns>A snapshot reflecting the fluent runtime model of the context.</returns>
        /// <remarks>
        /// <para>
        /// <b>Differences from <see cref="Build(Assembly)"/>:</b>
        /// </para>
        /// <list type="bullet">
        ///   <item>Uses the resolved <see cref="TableMapping"/> objects produced by the fluent model
        ///         builder. Fluent overrides (<c>ToTable()</c>, <c>HasColumnName()</c>, <c>HasKey()</c>,
        ///         <c>HasForeignKey()</c>, etc.) are fully reflected.</item>
        ///   <item>Only includes entity types explicitly registered via
        ///         <c>modelBuilder.Entity&lt;T&gt;()</c>. Types present in the assembly but not
        ///         registered are silently excluded, preventing StackOverflowExceptions from
        ///         circular navigation properties in test assemblies.</item>
        ///   <item>Includes foreign key constraints derived from fluent <c>HasForeignKey()</c>
        ///         / <c>HasOne()</c> / <c>HasMany()</c> configuration.</item>
        ///   <item>Includes inline owned scalar columns, shadow columns, and metadata from <c>OwnsOne()</c>,
        ///         owned collection child tables from <c>OwnsMany()</c>, and configured
        ///         many-to-many join tables.</item>
        /// </list>
        /// </remarks>
        public static SchemaSnapshot Build(DbContext ctx)
        {
            ArgumentNullException.ThrowIfNull(ctx);
            return BuildFromMappings(ctx.GetAllMappings());
        }

        /// <summary>
        /// Builds a <see cref="SchemaSnapshot"/> from a set of resolved <see cref="TableMapping"/> instances.
        /// </summary>
        private static SchemaSnapshot BuildFromMappings(IEnumerable<TableMapping> mappings)
        {
            ArgumentNullException.ThrowIfNull(mappings);

            var snapshot = new SchemaSnapshot();
            var allMappings = mappings as IReadOnlyList<TableMapping> ?? mappings.ToList();
            var tableByName = new Dictionary<string, TableSchema>(StringComparer.OrdinalIgnoreCase);

            // Pass 1: build all TableSchema objects, indexed by CLR type.
            var tableByType = new Dictionary<Type, TableSchema>();
            foreach (var map in allMappings)
            {
                var table = new TableSchema { Name = map.TableName };

                // Count PK columns so that composite PKs do not produce per-column UNIQUE constraints.
                var pkCount = map.Columns.Count(c => c.IsKey);
                var primaryKeyConstraintName = GetPrimaryKeyConstraintName(map.FluentConfiguration, map.TableName);
                var indexAttributesByProperty = BuildIndexAttributesByProperty(map.Columns);
                var indexColumnCounts = BuildIndexColumnCounts(indexAttributesByProperty);

                foreach (var col in map.Columns)
                {
                    var columnConfiguration = ResolveColumnConfiguration(map, col);
                    var clrType = Nullable.GetUnderlyingType(col.Prop.PropertyType) ?? col.Prop.PropertyType;
                    var isNullable = col.IsNullable;
                    var columnAttr = GetColumnAttribute(col);
                    var (precision, scale) = GetPrecision(col, clrType, columnConfiguration, columnAttr);
                    var storeFacets = ParseStringBinaryFacets(columnAttr?.TypeName, clrType);
                    ComputedColumnConfiguration? computedColumn = null;
                    columnConfiguration?.ComputedColumnSql.TryGetValue(col.Prop, out computedColumn);
                    IdentityOptionsConfiguration? identityOptions = null;
                    columnConfiguration?.IdentityOptions.TryGetValue(col.Prop, out identityOptions);
                    string? collation = null;
                    columnConfiguration?.Collations.TryGetValue(col.Prop, out collation);
                    var dbGenerated = GetDatabaseGeneratedOption(col);
                    var column = new ColumnSchema
                    {
                        Name         = col.Name,
                        // Use ToString() as fallback: for constructed generic types it returns the
                        // full generic form, which is unambiguous, unlike Name which gives just "List`1".
                        ClrType      = clrType.FullName ?? clrType.ToString(),
                        MaxLength    = GetMaxLength(col, clrType, columnConfiguration, columnAttr, storeFacets),
                        IsUnicode    = GetUnicode(col, clrType, columnConfiguration, storeFacets),
                        IsFixedLength = GetFixedLength(col, clrType, columnConfiguration, storeFacets),
                        Precision    = precision,
                        Scale        = scale,
                        IsNullable   = isNullable,
                        IsPrimaryKey = col.IsKey,
                        // Only mark IsUnique for single-column PKs; composite PKs must NOT
                        // emit per-column UNIQUE constraints.
                        IsUnique     = col.IsKey && pkCount == 1,
                        IndexName    = col.IsKey ? primaryKeyConstraintName : null,
                        IndexOrder   = null,
                        IsIdentity   = dbGenerated == DatabaseGeneratedOption.Identity,
                        IdentitySeed = identityOptions?.Seed,
                        IdentityIncrement = identityOptions?.Increment,
                        ComputedColumnSql = computedColumn?.Sql
                            ?? (dbGenerated == DatabaseGeneratedOption.Computed ? string.Empty : null),
                        IsStoredComputedColumn = computedColumn?.Stored == true,
                        DefaultValue = columnConfiguration?.DefaultValueSql.TryGetValue(col.Prop, out var defaultValue) == true
                            ? defaultValue
                            : null,
                        DefaultConstraintName = columnConfiguration?.DefaultValueConstraintNames.TryGetValue(col.Prop, out var defaultConstraintName) == true
                            ? defaultConstraintName
                            : null,
                        Collation = collation,
                    };
                    ApplyIndexAttributes(column, indexAttributesByProperty, indexColumnCounts, col.Prop);
                    table.Columns.Add(column);
                }
                if (map.FluentConfiguration is not null)
                {
                    AddConfiguredTableMetadata(table, map.FluentConfiguration);
                    AddConfiguredOwnedNavigationTableMetadata(table, map.FluentConfiguration);
                }
                snapshot.Tables.Add(table);
                tableByType[map.Type] = table;
                tableByName[table.Name] = table;
            }

            // Pass 2: add FK constraints from principal Relations to the dependent TableSchema.
            foreach (var map in allMappings)
            {
                foreach (var (_, rel) in map.Relations)
                {
                    if (!tableByType.TryGetValue(rel.DependentType, out var depTable))
                    {
                        // Design: silently skipped - dependent type is not registered in this
                        // context's mappings (e.g. it belongs to a different bounded context or
                        // was not configured via modelBuilder.Entity<T>()). The FK cannot be
                        // emitted without a known dependent table name.
                        WarnSkippedDependentType(rel.DependentType.Name, map.TableName);
                        continue;
                    }
                    depTable.ForeignKeys.Add(new ForeignKeySchema
                    {
                        ConstraintName   = !string.IsNullOrWhiteSpace(rel.ConstraintName)
                            ? rel.ConstraintName!
                            : $"FK_{depTable.Name}_{map.TableName}_{string.Join("_", rel.ForeignKeys.Select(c => c.Name))}",
                        DependentColumns = rel.ForeignKeys.Select(c => c.Name).ToArray(),
                        PrincipalTable   = map.TableName,
                        PrincipalColumns = rel.PrincipalKeys.Select(c => c.Name).ToArray(),
                        OnDelete         = ToForeignKeyActionSql(rel.OnDelete),
                        OnUpdate         = ToForeignKeyActionSql(rel.OnUpdate),
                    });
                }
            }

            // Pass 3: add configured owned collection tables so migrations create
            // the child tables used by runtime OwnsMany persistence.
            foreach (var map in allMappings)
            {
                foreach (var owned in map.OwnedCollections)
                {
                    var ownedTableName = FormatTableName(owned.TableName, owned.SchemaName);
                    if (tableByName.ContainsKey(ownedTableName))
                        continue;

                    if (map.KeyColumns.Length == 0)
                    {
                        WarnSkippedOwnedCollection(ownedTableName, map.Type.Name);
                        continue;
                    }

                    var ownerKey = ResolveOwnerKeyColumnForOwnedFk(map.KeyColumns, owned.ForeignKeyColumn, map.Type.Name);
                    var ownedTable = new TableSchema { Name = ownedTableName };
                    ownedTable.Columns.Add(BuildOwnedForeignKeyColumn(owned, ownerKey));

                    var ownedPkCount = owned.Columns.Count(static c => c.IsKey);
                    var ownedIndexAttributesByProperty = BuildIndexAttributesByProperty(owned.Columns);
                    var ownedIndexColumnCounts = BuildIndexColumnCounts(ownedIndexAttributesByProperty);
                    foreach (var col in owned.Columns)
                    {
                        if (string.Equals(col.Name, owned.ForeignKeyColumn, StringComparison.OrdinalIgnoreCase))
                            continue;

                        ownedTable.Columns.Add(BuildOwnedColumnSchema(
                            col,
                            ownedPkCount,
                            owned.TableName,
                            owned.Configuration,
                            ownedIndexAttributesByProperty,
                            ownedIndexColumnCounts));
                    }

                    AddConfiguredTableMetadata(ownedTable, owned.Configuration);
                    ownedTable.ForeignKeys.Add(new ForeignKeySchema
                    {
                        ConstraintName = $"FK_{owned.TableName}_{map.TableName}_{owned.ForeignKeyColumn}",
                        DependentColumns = new[] { owned.ForeignKeyColumn },
                        PrincipalTable = map.TableName,
                        PrincipalColumns = new[] { ownerKey.Name },
                        OnDelete = "CASCADE",
                        OnUpdate = "NO ACTION"
                    });

                    snapshot.Tables.Add(ownedTable);
                    tableByName[ownedTableName] = ownedTable;
                }
            }

            // Pass 4: add configured many-to-many join tables so migrations create the
            // bridge tables required by runtime skip-navigation mappings.
            foreach (var map in allMappings)
            {
                foreach (var join in map.ManyToManyJoins)
                {
                    var joinTableName = FormatTableName(join.TableName, join.SchemaName);
                    if (tableByName.ContainsKey(joinTableName))
                        continue;

                    if (!tableByType.TryGetValue(join.RightType, out var rightTable))
                    {
                        WarnSkippedManyToManyJoin(join.TableName, join.RightType.Name);
                        continue;
                    }

                    var joinTable = new TableSchema { Name = joinTableName };
                    var joinColumns = new Dictionary<string, ColumnSchema>(StringComparer.OrdinalIgnoreCase);
                    AddJoinColumns(joinTable, joinColumns, join.LeftFkColumns, join.LeftKeyColumns, joinTableName);
                    AddJoinColumns(joinTable, joinColumns, join.RightFkColumns, join.RightKeyColumns, joinTableName);

                    joinTable.ForeignKeys.Add(new ForeignKeySchema
                    {
                        ConstraintName   = $"FK_{join.TableName}_{map.TableName}_{string.Join("_", join.LeftFkColumns)}",
                        DependentColumns = join.LeftFkColumns.ToArray(),
                        PrincipalTable   = map.TableName,
                        PrincipalColumns = join.LeftKeyColumns.Select(c => c.Name).ToArray(),
                        OnDelete         = ToForeignKeyActionSql(join.LeftOnDelete),
                        OnUpdate         = ToForeignKeyActionSql(join.LeftOnUpdate)
                    });
                    joinTable.ForeignKeys.Add(new ForeignKeySchema
                    {
                        ConstraintName   = $"FK_{join.TableName}_{rightTable.Name}_{string.Join("_", join.RightFkColumns)}",
                        DependentColumns = join.RightFkColumns.ToArray(),
                        PrincipalTable   = rightTable.Name,
                        PrincipalColumns = join.RightKeyColumns.Select(c => c.Name).ToArray(),
                        OnDelete         = ToForeignKeyActionSql(join.RightOnDelete),
                        OnUpdate         = ToForeignKeyActionSql(join.RightOnUpdate)
                    });

                    snapshot.Tables.Add(joinTable);
                    tableByName[joinTableName] = joinTable;
                }
            }

            return snapshot;
        }

        private static ColumnSchema BuildOwnedForeignKeyColumn(OwnedCollectionMapping owned, Column ownerKey)
        {
            var clrType = Nullable.GetUnderlyingType(ownerKey.Prop.PropertyType) ?? ownerKey.Prop.PropertyType;
            return new ColumnSchema
            {
                Name = owned.ForeignKeyColumn,
                ClrType = clrType.FullName ?? clrType.ToString(),
                IsNullable = false,
                IsPrimaryKey = false,
                IsUnique = false,
                IndexName = null
            };
        }

        private static IEntityTypeConfiguration? ResolveColumnConfiguration(TableMapping map, Column column)
        {
            var configuration = map.FluentConfiguration;
            if (configuration is null)
                return null;

            if (column.Prop.DeclaringType == map.Type)
                return configuration;

            foreach (var (navigationProperty, ownedNavigation) in configuration.OwnedNavigations)
            {
                if (ownedNavigation.Configuration is null)
                    continue;
                if (column.Prop.DeclaringType != ownedNavigation.OwnedType)
                    continue;

                var prefix = navigationProperty.Name + "_";
                if (column.PropName.StartsWith(prefix, StringComparison.Ordinal))
                    return ownedNavigation.Configuration;
            }

            return configuration;
        }

        private static ColumnSchema BuildOwnedColumnSchema(
            Column col,
            int pkCount,
            string tableName,
            IEntityTypeConfiguration? configuration,
            IReadOnlyDictionary<PropertyInfo, IndexAttribute[]> indexAttributesByProperty,
            IReadOnlyDictionary<string, int> indexColumnCounts)
        {
            var clrType = Nullable.GetUnderlyingType(col.Prop.PropertyType) ?? col.Prop.PropertyType;
            var isNullable = col.IsNullable;
            var columnAttr = GetColumnAttribute(col);
            var (precision, scale) = GetPrecision(col, clrType, configuration, columnAttr);
            var storeFacets = ParseStringBinaryFacets(columnAttr?.TypeName, clrType);
            ComputedColumnConfiguration? computedColumn = null;
            configuration?.ComputedColumnSql.TryGetValue(col.Prop, out computedColumn);
            IdentityOptionsConfiguration? identityOptions = null;
            configuration?.IdentityOptions.TryGetValue(col.Prop, out identityOptions);
            string? collation = null;
            configuration?.Collations.TryGetValue(col.Prop, out collation);
            var dbGenerated = GetDatabaseGeneratedOption(col);
            var primaryKeyConstraintName = GetPrimaryKeyConstraintName(configuration, tableName);
            var column = new ColumnSchema
            {
                Name = col.Name,
                ClrType = clrType.FullName ?? clrType.ToString(),
                MaxLength = GetMaxLength(col, clrType, configuration, columnAttr, storeFacets),
                IsUnicode = GetUnicode(col, clrType, configuration, storeFacets),
                IsFixedLength = GetFixedLength(col, clrType, configuration, storeFacets),
                Precision = precision,
                Scale = scale,
                IsNullable = isNullable,
                IsPrimaryKey = col.IsKey,
                IsUnique = col.IsKey && pkCount == 1,
                IndexName = col.IsKey ? primaryKeyConstraintName : null,
                IsIdentity = dbGenerated == DatabaseGeneratedOption.Identity,
                IdentitySeed = identityOptions?.Seed,
                IdentityIncrement = identityOptions?.Increment,
                ComputedColumnSql = computedColumn?.Sql
                    ?? (dbGenerated == DatabaseGeneratedOption.Computed ? string.Empty : null),
                IsStoredComputedColumn = computedColumn?.Stored == true,
                DefaultValue = configuration?.DefaultValueSql.TryGetValue(col.Prop, out var defaultValue) == true
                    ? defaultValue
                    : null,
                DefaultConstraintName = configuration?.DefaultValueConstraintNames.TryGetValue(col.Prop, out var defaultConstraintName) == true
                    ? defaultConstraintName
                    : null,
                Collation = collation
            };
            ApplyIndexAttributes(column, indexAttributesByProperty, indexColumnCounts, col.Prop);
            return column;
        }

        private static string GetPrimaryKeyConstraintName(IEntityTypeConfiguration? configuration, string tableName)
            => !string.IsNullOrWhiteSpace(configuration?.PrimaryKeyConstraintName)
                ? configuration.PrimaryKeyConstraintName!
                : $"PK_{tableName}";

        private static void AddConfiguredOwnedNavigationTableMetadata(TableSchema table, IEntityTypeConfiguration configuration)
        {
            foreach (var ownedNavigation in configuration.OwnedNavigations.Values)
            {
                AddConfiguredTableMetadata(table, ownedNavigation.Configuration);
            }
        }

        private static void AddConfiguredTableMetadata(TableSchema table, IEntityTypeConfiguration? configuration)
        {
            if (configuration is null)
                return;

            foreach (var check in configuration.CheckConstraints)
            {
                table.CheckConstraints.Add(new CheckConstraintSchema
                {
                    ConstraintName = check.Name,
                    Sql = check.Sql
                });
            }

            foreach (var expressionIndex in configuration.ExpressionIndexes)
            {
                table.ExpressionIndexes.Add(new ExpressionIndexSchema
                {
                    Name = expressionIndex.Name,
                    ExpressionSql = expressionIndex.ExpressionSql,
                    IsUnique = expressionIndex.IsUnique,
                    FilterSql = expressionIndex.FilterSql,
                    IncludedColumnNames = expressionIndex.IncludedColumnNames,
                    NullSortOrder = expressionIndex.NullSortOrder,
                    NullsNotDistinct = expressionIndex.NullsNotDistinct
                });
            }
        }

        private static ColumnAttribute? GetColumnAttribute(Column column)
            => column.IsShadow ? null : column.Prop.GetCustomAttribute<ColumnAttribute>();

        private static DatabaseGeneratedOption? GetDatabaseGeneratedOption(Column column)
            => column.IsShadow
                ? null
                : column.Prop.GetCustomAttribute<DatabaseGeneratedAttribute>()?.DatabaseGeneratedOption;

        private static int? GetMaxLength(Column column, Type clrType)
            => column.IsShadow
                ? null
                : GetMaxLength(column.Prop, clrType, GetColumnAttribute(column), ParseStringBinaryFacets(GetColumnAttribute(column)?.TypeName, clrType));

        private static int? GetMaxLength(
            Column column,
            Type clrType,
            IEntityTypeConfiguration? configuration,
            ColumnAttribute? columnAttribute,
            ScaffoldStringBinaryFacets storeFacets)
        {
            if (!column.IsShadow
                && configuration?.MaxLengths.TryGetValue(column.Prop, out var configuredMaxLength) == true)
            {
                return configuredMaxLength;
            }

            return column.IsShadow
                ? null
                : GetMaxLength(column.Prop, clrType, columnAttribute, storeFacets);
        }

        private static int? GetMaxLength(
            PropertyInfo property,
            Type clrType,
            ColumnAttribute? columnAttribute,
            ScaffoldStringBinaryFacets storeFacets)
        {
            if (clrType != typeof(string) && clrType != typeof(byte[]))
                return null;

            var maxLengthAttribute = property.GetCustomAttribute<MaxLengthAttribute>();
            var length = maxLengthAttribute?.Length > 0
                ? maxLengthAttribute.Length
                : (int?)null;

            if (clrType == typeof(string))
            {
                var stringLengthAttribute = property.GetCustomAttribute<StringLengthAttribute>();
                if (stringLengthAttribute?.MaximumLength > 0)
                {
                    length = length.HasValue
                        ? Math.Min(length.Value, stringLengthAttribute.MaximumLength)
                        : stringLengthAttribute.MaximumLength;
                }
            }

            return length ?? storeFacets.MaxLength;
        }

        private static bool? GetUnicode(
            Column column,
            Type clrType,
            IEntityTypeConfiguration? configuration,
            ScaffoldStringBinaryFacets storeFacets)
        {
            if (clrType != typeof(string))
                return null;

            if (!column.IsShadow
                && configuration?.UnicodeSettings.TryGetValue(column.Prop, out var configuredUnicode) == true)
            {
                return configuredUnicode;
            }

            return storeFacets.IsUnicode;
        }

        private static bool GetFixedLength(
            Column column,
            Type clrType,
            IEntityTypeConfiguration? configuration,
            ScaffoldStringBinaryFacets storeFacets)
        {
            if (clrType != typeof(string) && clrType != typeof(byte[]))
                return false;

            if (!column.IsShadow
                && configuration?.FixedLengthSettings.TryGetValue(column.Prop, out var configuredFixedLength) == true)
            {
                return configuredFixedLength;
            }

            return storeFacets.IsFixedLength;
        }

        private static (int? Precision, int? Scale) GetPrecision(
            Column column,
            Type clrType,
            IEntityTypeConfiguration? configuration,
            ColumnAttribute? columnAttribute)
        {
            if (!column.IsShadow
                && configuration?.Precisions.TryGetValue(column.Prop, out var configuredPrecision) == true)
            {
                return (configuredPrecision.Precision, configuredPrecision.Scale);
            }

            return TryParseDecimalPrecision(columnAttribute?.TypeName, clrType);
        }

        private static Dictionary<PropertyInfo, IndexAttribute[]> BuildIndexAttributesByProperty(IEnumerable<Column> columns)
            => columns
                .Where(static column => !column.IsShadow)
                .Select(static column => column.Prop)
                .Distinct()
                .ToDictionary(
                    static prop => prop,
                    static prop => prop.GetCustomAttributes<IndexAttribute>().ToArray());

        private static Dictionary<string, int> BuildIndexColumnCounts(IReadOnlyDictionary<PropertyInfo, IndexAttribute[]> indexAttributesByProperty)
            => indexAttributesByProperty.Values
                .SelectMany(static attrs => attrs.Select(static attr => attr.Name))
                .GroupBy(static name => name, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(static group => group.Key, static group => group.Count(), StringComparer.OrdinalIgnoreCase);

        private static void ApplyIndexAttributes(
            ColumnSchema column,
            IReadOnlyDictionary<PropertyInfo, IndexAttribute[]> indexAttributesByProperty,
            IReadOnlyDictionary<string, int> indexColumnCounts,
            PropertyInfo property)
        {
            if (!indexAttributesByProperty.TryGetValue(property, out var indexAttrs) || indexAttrs.Length == 0)
                return;

            var firstIndexAttr = indexAttrs[0];
            if (!column.IsPrimaryKey)
            {
                column.IndexName = firstIndexAttr.Name;
                column.IndexOrder = firstIndexAttr.Order;
            }

            if (indexAttrs.Any(attr =>
                    attr.IsUnique &&
                    indexColumnCounts.TryGetValue(attr.Name, out var count) &&
                    count == 1))
            {
                column.IsUnique = true;
            }

            foreach (var indexAttr in indexAttrs)
            {
                column.Indexes.Add(new ColumnIndexSchema
                {
                    Name = indexAttr.Name,
                    IsUnique = indexAttr.IsUnique,
                    Order = indexAttr.Order,
                    IsDescending = indexAttr.IsDescending,
                    IsIncluded = indexAttr.IsIncluded,
                    NullsNotDistinct = indexAttr.NullsNotDistinct,
                    NullSortOrder = indexAttr.NullSortOrder,
                    FilterSql = indexAttr.FilterSql
                });
            }
        }

        private static Column ResolveOwnerKeyColumnForOwnedFk(Column[] ownerKeyColumns, string ownedFkColumnName, string ownerTypeName)
        {
            if (ownerKeyColumns.Length == 1)
                return ownerKeyColumns[0];

            var match = Array.Find(ownerKeyColumns, c =>
                string.Equals(c.Name, ownedFkColumnName, StringComparison.OrdinalIgnoreCase));
            if (match != null)
                return match;

            if (ownedFkColumnName.Length > ownerTypeName.Length
                && ownedFkColumnName.StartsWith(ownerTypeName, StringComparison.OrdinalIgnoreCase))
            {
                var suffix = ownedFkColumnName.Substring(ownerTypeName.Length);
                match = Array.Find(ownerKeyColumns, c =>
                    string.Equals(c.Name, suffix, StringComparison.OrdinalIgnoreCase));
                if (match != null)
                    return match;
            }

            return ownerKeyColumns[0];
        }

        private static void AddJoinColumns(
            TableSchema joinTable,
            Dictionary<string, ColumnSchema> columnsByName,
            IReadOnlyList<string> fkColumns,
            IReadOnlyList<Column> principalColumns,
            string joinTableName)
        {
            for (var i = 0; i < fkColumns.Count; i++)
            {
                var fkColumn = fkColumns[i];
                if (columnsByName.ContainsKey(fkColumn))
                    continue;

                var principalColumn = principalColumns[i];
                var clrType = Nullable.GetUnderlyingType(principalColumn.Prop.PropertyType) ?? principalColumn.Prop.PropertyType;
                var column = new ColumnSchema
                {
                    Name         = fkColumn,
                    ClrType      = clrType.FullName ?? clrType.ToString(),
                    IsNullable   = false,
                    IsPrimaryKey = true,
                    IsUnique     = false,
                    IndexName    = $"PK_{joinTableName}"
                };
                columnsByName[fkColumn] = column;
                joinTable.Columns.Add(column);
            }
        }

        private static string FormatTableName(string tableName, string? schemaName)
            => string.IsNullOrWhiteSpace(schemaName) ? tableName : schemaName + "." + tableName;

        private static string ToForeignKeyActionSql(ReferentialAction action)
            => action switch
            {
                ReferentialAction.Cascade => "CASCADE",
                ReferentialAction.SetNull => "SET NULL",
                ReferentialAction.Restrict => "RESTRICT",
                ReferentialAction.SetDefault => "SET DEFAULT",
                _ => "NO ACTION"
            };

        /// <summary>
        /// Returns entity candidate types from the assembly: non-abstract classes (including
        /// nested types) that carry a <see cref="TableAttribute"/> or at least one
        /// <see cref="KeyAttribute"/> property. No visibility filter is applied because nested
        /// public types report <c>IsNestedPublic</c> rather than <c>IsPublic</c>.
        /// </summary>
        private static IEnumerable<Type> GetEntityCandidates(Assembly assembly)
        {
            Type[] types;
            try
            {
                types = assembly.GetTypes();
            }
            catch (ReflectionTypeLoadException ex)
            {
                // Some types in the assembly may fail to load (e.g. missing dependencies).
                // Use the successfully loaded subset rather than failing the entire snapshot.
                types = (ex.Types ?? Array.Empty<Type?>()).Where(t => t != null).Select(t => t!).ToArray();
            }

            return types.Where(t =>
                t.IsClass && !t.IsAbstract &&
                (t.GetCustomAttribute<TableAttribute>() != null ||
                 t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                  .Any(p => p.GetCustomAttribute<KeyAttribute>() != null)));
        }

        /// <summary>
        /// Returns true for types that map to database scalar columns.
        /// Reference navigation properties and collection properties return false.
        /// </summary>
        private static bool IsMappableType(Type t)
        {
            // Unwrap Nullable<T> - e.g. int?, Guid?
            var underlying = Nullable.GetUnderlyingType(t);
            if (underlying != null)
                return true; // Nullable value types are always mappable

            // Value types (int, bool, DateTime, Guid, enum, etc.) are always mappable
            if (t.IsValueType)
                return true;

            // string and byte[] are the only reference types that are scalar columns
            if (t == typeof(string) || t == typeof(byte[]))
                return true;

            // All other reference types (class, interface) are navigation properties
            return false;
        }

        private static string GetTableName(Type type, TableAttribute? tableAttribute)
        {
            if (tableAttribute is null)
                return type.Name;

            return string.IsNullOrWhiteSpace(tableAttribute.Schema)
                ? tableAttribute.Name
                : tableAttribute.Schema + "." + tableAttribute.Name;
        }

        private readonly record struct ScaffoldStringBinaryFacets(int? MaxLength, bool? IsUnicode, bool IsFixedLength);

        private static ScaffoldStringBinaryFacets ParseStringBinaryFacets(string? typeName, Type clrType)
        {
            if ((clrType != typeof(string) && clrType != typeof(byte[]))
                || string.IsNullOrWhiteSpace(typeName))
            {
                return default;
            }

            var normalized = NormalizeStoreTypeName(typeName);
            var open = normalized.LastIndexOf('(');
            var close = open >= 0 ? normalized.IndexOf(')', open + 1) : -1;
            if ((open >= 0 && close != normalized.Length - 1)
                || (open < 0 && normalized.Contains(')', StringComparison.Ordinal)))
            {
                return default;
            }

            var baseName = (open >= 0 ? normalized[..open] : normalized).Trim();
            var args = open >= 0 && close > open
                ? normalized.Substring(open + 1, close - open - 1)
                    .Split(',', StringSplitOptions.TrimEntries)
                : Array.Empty<string>();
            if (args.Length > 1 || args.Any(static arg => arg.Length == 0))
                return default;

            var maxLength = args.Length == 1
                && !string.Equals(args[0], "max", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(args[0], NumberStyles.None, CultureInfo.InvariantCulture, out var parsedLength)
                && parsedLength > 0
                ? parsedLength
                : (int?)null;

            return clrType == typeof(byte[])
                ? baseName switch
                {
                    "binary" => new ScaffoldStringBinaryFacets(maxLength, null, true),
                    "varbinary" => new ScaffoldStringBinaryFacets(maxLength, null, false),
                    _ => default
                }
                : baseName switch
                {
                    "nchar" or "national character" => new ScaffoldStringBinaryFacets(maxLength, true, true),
                    "nvarchar" or "national character varying" => new ScaffoldStringBinaryFacets(maxLength, true, false),
                    "char" => new ScaffoldStringBinaryFacets(maxLength, false, true),
                    "varchar" => new ScaffoldStringBinaryFacets(maxLength, false, false),
                    "text" => new ScaffoldStringBinaryFacets(null, false, false),
                    "ntext" => new ScaffoldStringBinaryFacets(null, true, false),
                    "character" => new ScaffoldStringBinaryFacets(maxLength, null, true),
                    "character varying" or "varying character" => new ScaffoldStringBinaryFacets(maxLength, null, false),
                    _ => default
                };
        }

        private static string NormalizeStoreTypeName(string typeName)
        {
            var normalized = typeName.Trim().ToLowerInvariant();
            if (normalized.StartsWith("domain (", StringComparison.Ordinal) && normalized.EndsWith(")", StringComparison.Ordinal))
            {
                var arrow = normalized.LastIndexOf("->", StringComparison.Ordinal);
                if (arrow >= 0)
                    normalized = normalized.Substring(arrow + 2, normalized.Length - arrow - 3).Trim();
            }

            var open = normalized.IndexOf('(');
            var baseName = open >= 0 ? normalized[..open].Trim() : normalized;
            var lastDot = baseName.LastIndexOf('.');
            if (lastDot >= 0 && lastDot + 1 < baseName.Length)
                normalized = baseName[(lastDot + 1)..] + (open >= 0 ? normalized[open..] : string.Empty);

            return normalized;
        }

        private static (int? Precision, int? Scale) TryParseDecimalPrecision(string? typeName, Type clrType)
        {
            if (clrType != typeof(decimal) || string.IsNullOrWhiteSpace(typeName))
                return (null, null);

            var open = typeName.LastIndexOf('(');
            var close = typeName.IndexOf(')', open + 1);
            if (open < 0 || close < 0)
                return (null, null);

            var baseName = typeName[..open].Trim();
            if (!EndsWithDelimitedTypeName(baseName, "decimal")
                && !EndsWithDelimitedTypeName(baseName, "numeric"))
            {
                return (null, null);
            }

            var precisionAndScaleText = typeName.Substring(open + 1, close - open - 1);
            var parts = precisionAndScaleText
                .Split(',', StringSplitOptions.TrimEntries);
            if (parts.Length is < 1 or > 2)
                return (null, null);

            if (int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out var precision)
                && precision > 0)
            {
                if (parts.Length == 1)
                    return (precision, null);

                if (int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var scale)
                    && scale >= 0
                    && scale <= precision)
                {
                    return (precision, scale);
                }
            }

            return (null, null);
        }

        private static bool EndsWithDelimitedTypeName(string text, string typeName)
        {
            var trimmed = text.TrimEnd();
            if (!trimmed.EndsWith(typeName, StringComparison.OrdinalIgnoreCase))
                return false;

            var prefixLength = trimmed.Length - typeName.Length;
            return prefixLength == 0 || !IsTypeNameIdentifierChar(trimmed[prefixLength - 1]);
        }

        private static bool IsTypeNameIdentifierChar(char value)
            => char.IsLetterOrDigit(value) || value == '_';

        /// <summary>
        /// Emits a debug diagnostic when a FK's dependent type is not registered in the current
        /// context's mappings and must be silently skipped during snapshot construction.
        /// Active only in DEBUG builds; no-op in Release.
        /// </summary>
        [Conditional("DEBUG")]
        private static void WarnSkippedDependentType(string dependentTypeName, string principalTableName)
        {
            Debug.WriteLine(
                $"[SchemaSnapshot] FK skipped: dependent type '{dependentTypeName}' is not " +
                $"registered in this context's mappings (principal table: '{principalTableName}'). " +
                "Register the dependent type via modelBuilder.Entity<T>() to include its FK.");
        }

        /// <summary>
        /// Emits a debug diagnostic when a configured many-to-many join cannot be snapshotted
        /// because the related mapping is not present in the current mapping set.
        /// Active only in DEBUG builds; no-op in Release.
        /// </summary>
        [Conditional("DEBUG")]
        private static void WarnSkippedManyToManyJoin(string joinTableName, string relatedTypeName)
        {
            Debug.WriteLine(
                $"[SchemaSnapshot] many-to-many join table '{joinTableName}' skipped: " +
                $"related type '{relatedTypeName}' is not registered in this context's mappings.");
        }

        /// <summary>
        /// Emits a debug diagnostic when a configured owned collection cannot be snapshotted
        /// because the owner mapping has no primary key metadata.
        /// Active only in DEBUG builds; no-op in Release.
        /// </summary>
        [Conditional("DEBUG")]
        private static void WarnSkippedOwnedCollection(string ownedTableName, string ownerTypeName)
        {
            Debug.WriteLine(
                $"[SchemaSnapshot] owned collection table '{ownedTableName}' skipped: " +
                $"owner type '{ownerTypeName}' has no primary key metadata.");
        }
    }
}
