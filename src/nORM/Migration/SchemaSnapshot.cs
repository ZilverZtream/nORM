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
    /// Represents the desired database schema state derived from code mappings (attributes or
    /// fluent configuration). This is NOT a live snapshot of an actual database — it reflects
    /// what the schema <em>should</em> look like based on the current entity model.
    /// Use <see cref="SchemaDiffer.Diff"/> to compare two snapshots and produce the migrations
    /// required to bring an old schema up to date with a new one.
    /// </summary>
    public class SchemaSnapshot
    {
        /// <summary>Tables captured in the snapshot.</summary>
        public List<TableSchema> Tables { get; init; } = new();
    }

    /// <summary>
    /// Describes the schema of a single table including its columns and foreign keys.
    /// </summary>
    public class TableSchema
    {
        /// <summary>Name of the table.</summary>
        public string Name { get; set; } = string.Empty;
        /// <summary>Columns defined on the table.</summary>
        public List<ColumnSchema> Columns { get; init; } = new();
        /// <summary>Foreign key constraints defined on this table.</summary>
        public List<ForeignKeySchema> ForeignKeys { get; init; } = new();
    }

    /// <summary>
    /// Describes a foreign key constraint between a dependent (child) table and a principal (parent) table.
    /// </summary>
    public class ForeignKeySchema
    {
        /// <summary>Name of the constraint (e.g. "FK_Post_Blog_BlogId").</summary>
        public string ConstraintName { get; set; } = string.Empty;
        /// <summary>Columns on the dependent (child) table that hold the FK values. Order is significant.</summary>
        public string[] DependentColumns { get; set; } = Array.Empty<string>();
        /// <summary>The principal (parent) table being referenced.</summary>
        public string PrincipalTable { get; set; } = string.Empty;
        /// <summary>Columns on the principal table being referenced (usually the PK). Order matches DependentColumns.</summary>
        public string[] PrincipalColumns { get; set; } = Array.Empty<string>();
        /// <summary>
        /// Referential action on DELETE (NO ACTION, CASCADE, SET NULL, RESTRICT, SET DEFAULT).
        /// "NO ACTION" is the default and causes no ON DELETE clause to be emitted.
        /// </summary>
        public string OnDelete { get; set; } = "NO ACTION";
        /// <summary>
        /// Referential action on UPDATE (NO ACTION, CASCADE, SET NULL, RESTRICT, SET DEFAULT).
        /// "NO ACTION" is the default and causes no ON UPDATE clause to be emitted.
        /// </summary>
        public string OnUpdate { get; set; } = "NO ACTION";
    }

    /// <summary>
    /// Describes a column within a table schema snapshot.
    /// </summary>
    public class ColumnSchema
    {
        /// <summary>Name of the column.</summary>
        public string Name { get; set; } = string.Empty;
        /// <summary>
        /// When non-null, indicates that this column was previously named <see cref="PreviousName"/>
        /// and the schema differ should emit a RENAME COLUMN operation instead of DROP + ADD.
        /// Populated by <see cref="SchemaSnapshotBuilder"/> when a property carries
        /// <c>[RenameColumn("oldName")]</c>.
        /// </summary>
        public string? PreviousName { get; set; }
        /// <summary>
        /// Full CLR type name of the column (e.g. <c>System.Int32</c>).
        /// An empty string is a recognizable placeholder meaning "unknown/unresolved type";
        /// callers that produce <see cref="ColumnSchema"/> instances should always populate this
        /// field — leaving it empty will cause all four SQL generators to fall back to their
        /// default type (e.g. NVARCHAR(MAX) for SQL Server) and may suppress spurious alter
        /// detections because two columns with an empty ClrType compare equal.
        /// </summary>
        // NOTE: the default is intentionally string.Empty (not null) so that null-safe string
        // comparisons in SchemaDiffer.Diff do not require extra null guards. If ClrType is empty
        // on a ColumnSchema produced by external code, treat it as a configuration concern.
        public string ClrType { get; set; } = string.Empty;
        /// <summary>Optional decimal/numeric precision for providers that support fixed-precision decimals.</summary>
        public int? Precision { get; set; }
        /// <summary>Optional decimal/numeric scale for providers that support fixed-precision decimals.</summary>
        public int? Scale { get; set; }
        /// <summary>Indicates whether the column allows <c>null</c> values.</summary>
        public bool IsNullable { get; set; }
        /// <summary>True when the column is (part of) the table's primary key.</summary>
        public bool IsPrimaryKey { get; set; }
        /// <summary>True when the column has a UNIQUE index.</summary>
        public bool IsUnique { get; set; }
        /// <summary>Non-null means the column is covered by a named index.</summary>
        public string? IndexName { get; set; }
        /// <summary>Zero-based order of this column within a named composite index.</summary>
        public int? IndexOrder { get; set; }
        /// <summary>Named indexes this column participates in.</summary>
        public List<ColumnIndexSchema> Indexes { get; } = new();
        /// <summary>SQL literal default value for ADD COLUMN NOT NULL migrations (e.g. "''" or "0").</summary>
        public string? DefaultValue { get; set; }
        /// <summary>True when the column has identity/autoincrement semantics (e.g. [DatabaseGenerated(Identity)]).</summary>
        public bool IsIdentity { get; set; }
    }

    /// <summary>
    /// Describes one index membership for a column.
    /// </summary>
    public class ColumnIndexSchema
    {
        /// <summary>Database index name.</summary>
        public string Name { get; set; } = string.Empty;
        /// <summary>True when the named index enforces uniqueness.</summary>
        public bool IsUnique { get; set; }
        /// <summary>Zero-based order of this column within the named index.</summary>
        public int? Order { get; set; }
        /// <summary>True when this index key column is ordered descending.</summary>
        public bool IsDescending { get; set; }
        /// <summary>True when this column is an included, non-key column.</summary>
        public bool IsIncluded { get; set; }
    }

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
        ///         same <c>[Table]</c> name — see the duplicate-name guard for details.</item>
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
                    // Exclude properties with no setter at all — these are computed/expression-body
                    // properties (e.g. public string FullName => FirstName + " " + LastName) and have
                    // no backing database column. Note: init-only properties DO have a setter
                    // (IsInitOnly=true), so CanWrite returns true for them and they are included.
                    if (!prop.CanWrite)
                        continue;
                    if (prop.GetCustomAttribute<NotMappedAttribute>() != null)
                        continue;
                    // Exclude collection/enumerable navigation properties (e.g. List<Post>, ICollection<T>)
                    if (typeof(System.Collections.IEnumerable).IsAssignableFrom(prop.PropertyType)
                        && prop.PropertyType != typeof(string))
                        continue;
                    // Exclude reference navigation properties (class types that are not mappable scalars)
                    if (!IsMappableType(prop.PropertyType))
                        continue;

                    var colAttr = prop.GetCustomAttribute<ColumnAttribute>();
                    var clr = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                    // Skip open generic type parameters (e.g. T in MyEntity<T>) — FullName is null
                    // and the parameter name (e.g. "T") is ambiguous and not a valid SQL type.
                    if (clr.IsGenericTypeParameter)
                        continue;
                    var isPk = pkNames.Contains(prop.Name);
                    var dbGenAttr = prop.GetCustomAttribute<DatabaseGeneratedAttribute>();
                    var renameAttr = prop.GetCustomAttribute<RenameColumnAttr>();
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
                        // Treat both Identity and Computed as server-managed columns for snapshot
                        // purposes: both are excluded from INSERT/UPDATE, and both require the
                        // migration generator to omit a NOT NULL constraint without a DEFAULT.
                        IsIdentity = dbGenAttr?.DatabaseGeneratedOption == DatabaseGeneratedOption.Identity
                                  || dbGenAttr?.DatabaseGeneratedOption == DatabaseGeneratedOption.Computed,
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
                            IsIncluded = indexAttr.IsIncluded
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
        ///   <item>Owned collection tables and many-to-many join tables are not included — see
        ///         <see cref="BuildFromMappings"/> remarks for details.</item>
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
        /// <remarks>
        /// <b>Architectural limitations — tables not included in the snapshot:</b>
        /// <list type="bullet">
        ///   <item>
        ///     <b>Owned collection tables</b> (configured via <c>OwnedCollectionMapping</c>) are not
        ///     represented in <see cref="TableMapping"/> and are therefore absent from the snapshot.
        ///     Migrations for owned collection tables must be managed manually.
        ///   </item>
        ///   <item>
        ///     <b>Many-to-many join tables</b> (implicit or explicit) are not included.
        ///     Migrations for join tables must be managed manually.
        ///   </item>
        /// </list>
        /// </remarks>
        private static SchemaSnapshot BuildFromMappings(IEnumerable<TableMapping> mappings)
        {
            ArgumentNullException.ThrowIfNull(mappings);

            var snapshot = new SchemaSnapshot();
            var allMappings = mappings as IReadOnlyList<TableMapping> ?? mappings.ToList();

            // Pass 1: build all TableSchema objects, indexed by CLR type.
            var tableByType = new Dictionary<Type, TableSchema>();
            foreach (var map in allMappings)
            {
                var table = new TableSchema { Name = map.TableName };

                // Count PK columns so that composite PKs do not produce per-column UNIQUE constraints.
                var pkCount = map.Columns.Count(c => c.IsKey);

                foreach (var col in map.Columns)
                {
                    var clrType = Nullable.GetUnderlyingType(col.Prop.PropertyType) ?? col.Prop.PropertyType;
                    var isNullable = !col.Prop.PropertyType.IsValueType
                                  || Nullable.GetUnderlyingType(col.Prop.PropertyType) != null;
                    var columnAttr = col.Prop.GetCustomAttribute<ColumnAttribute>();
                    var (precision, scale) = TryParseDecimalPrecision(columnAttr?.TypeName, clrType);
                    table.Columns.Add(new ColumnSchema
                    {
                        Name         = col.Name,
                        // Use ToString() as fallback: for constructed generic types it returns the
                        // full generic form, which is unambiguous, unlike Name which gives just "List`1".
                        ClrType      = clrType.FullName ?? clrType.ToString(),
                        Precision    = precision,
                        Scale        = scale,
                        IsNullable   = isNullable,
                        IsPrimaryKey = col.IsKey,
                        // Only mark IsUnique for single-column PKs; composite PKs must NOT
                        // emit per-column UNIQUE constraints.
                        IsUnique     = col.IsKey && pkCount == 1,
                        IndexName    = col.IsKey ? $"PK_{map.TableName}" : null,
                        IndexOrder   = null,
                        IsIdentity   = col.IsDbGenerated,
                        DefaultValue = map.FluentConfiguration?.DefaultValueSql.TryGetValue(col.Prop, out var defaultValue) == true
                            ? defaultValue
                            : null,
                    });
                }
                snapshot.Tables.Add(table);
                tableByType[map.Type] = table;
            }

            // Pass 2: add FK constraints from principal Relations to the dependent TableSchema.
            foreach (var map in allMappings)
            {
                foreach (var (_, rel) in map.Relations)
                {
                    if (!tableByType.TryGetValue(rel.DependentType, out var depTable))
                    {
                        // Design: silently skipped — dependent type is not registered in this
                        // context's mappings (e.g. it belongs to a different bounded context or
                        // was not configured via modelBuilder.Entity<T>()). The FK cannot be
                        // emitted without a known dependent table name.
                        WarnSkippedDependentType(rel.DependentType.Name, map.TableName);
                        continue;
                    }
                    depTable.ForeignKeys.Add(new ForeignKeySchema
                    {
                        ConstraintName   = $"FK_{depTable.Name}_{map.TableName}_{string.Join("_", rel.ForeignKeys.Select(c => c.Name))}",
                        DependentColumns = rel.ForeignKeys.Select(c => c.Name).ToArray(),
                        PrincipalTable   = map.TableName,
                        PrincipalColumns = rel.PrincipalKeys.Select(c => c.Name).ToArray(),
                        OnDelete         = ToForeignKeyActionSql(rel.OnDelete),
                        OnUpdate         = ToForeignKeyActionSql(rel.OnUpdate),
                    });
                }
            }

            return snapshot;
        }

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
            // Unwrap Nullable<T> — e.g. int?, Guid?
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

        private static (int? Precision, int? Scale) TryParseDecimalPrecision(string? typeName, Type clrType)
        {
            if (clrType != typeof(decimal) || string.IsNullOrWhiteSpace(typeName))
                return (null, null);

            var open = typeName.IndexOf('(');
            var comma = typeName.IndexOf(',', open + 1);
            var close = typeName.IndexOf(')', comma + 1);
            if (open < 0 || comma < 0 || close < 0)
                return (null, null);

            var baseName = typeName[..open].Trim();
            if (!baseName.EndsWith("decimal", StringComparison.OrdinalIgnoreCase)
                && !baseName.EndsWith("numeric", StringComparison.OrdinalIgnoreCase))
            {
                return (null, null);
            }

            if (int.TryParse(typeName.AsSpan(open + 1, comma - open - 1), NumberStyles.None, CultureInfo.InvariantCulture, out var precision)
                && int.TryParse(typeName.AsSpan(comma + 1, close - comma - 1), NumberStyles.None, CultureInfo.InvariantCulture, out var scale)
                && precision > 0
                && scale >= 0
                && scale <= precision)
            {
                return (precision, scale);
            }

            return (null, null);
        }

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
    }

    /// <summary>
    /// Represents the differences between two <see cref="SchemaSnapshot"/> instances.
    /// </summary>
    /// <remarks>
    /// All list properties on this class are initialized to non-null empty lists and must
    /// remain non-null throughout their lifetime. Individual entries within each list must
    /// also be non-null; adding a null entry will cause downstream migration SQL generators
    /// to throw a <see cref="NullReferenceException"/>. The lists themselves must never be
    /// set to null — <see cref="HasChanges"/> and all four SQL generators assume non-null lists.
    /// </remarks>
    public class SchemaDiff
    {
        /// <summary>Tables that exist in the new snapshot but not in the old.</summary>
        public List<TableSchema> AddedTables { get; } = new();
        /// <summary>Columns that exist in the new snapshot but not in the old.</summary>
        public List<(TableSchema Table, ColumnSchema Column)> AddedColumns { get; } = new();
        /// <summary>Columns whose definition has changed between snapshots.</summary>
        public List<(TableSchema Table, ColumnSchema NewColumn, ColumnSchema OldColumn)> AlteredColumns { get; } = new();
        /// <summary>Indexes that appear in the new snapshot but not in the old.</summary>
        public List<(TableSchema Table, string IndexName, bool IsUnique, string[] ColumnNames, bool[] Descending)> AddedIndexes { get; } = new();
        /// <summary>Indexes that appear in the old snapshot but not in the new.</summary>
        public List<(TableSchema Table, string IndexName)> DroppedIndexes { get; } = new();
        /// <summary>Tables that exist in the old snapshot but not in the new (dropped tables).</summary>
        public List<TableSchema> DroppedTables { get; } = new();
        /// <summary>Columns that exist in the old snapshot but not in the new for a given table (dropped columns).</summary>
        public List<(TableSchema Table, ColumnSchema Column)> DroppedColumns { get; } = new();
        /// <summary>FK constraints present in the new snapshot but not the old for an existing table.</summary>
        public List<(TableSchema Table, ForeignKeySchema ForeignKey)> AddedForeignKeys { get; } = new();
        /// <summary>FK constraints present in the old snapshot but not the new for an existing table.</summary>
        public List<(TableSchema Table, ForeignKeySchema ForeignKey)> DroppedForeignKeys { get; } = new();
        /// <summary>
        /// Column renames detected via <c>[RenameColumn("oldName")]</c>. Each entry carries the table,
        /// the old column name, and the new <see cref="ColumnSchema"/> (with its new name). The differ
        /// populates this list instead of adding to <see cref="DroppedColumns"/> / <see cref="AddedColumns"/>.
        /// </summary>
        public List<(TableSchema Table, string OldColumnName, ColumnSchema NewColumn)> RenamedColumns { get; } = new();

        /// <summary>Indicates whether the diff contains any schema changes.</summary>
        public bool HasChanges => AddedTables.Count > 0 || AddedColumns.Count > 0 || AlteredColumns.Count > 0
            || AddedIndexes.Count > 0 || DroppedIndexes.Count > 0
            || DroppedTables.Count > 0 || DroppedColumns.Count > 0
            || AddedForeignKeys.Count > 0 || DroppedForeignKeys.Count > 0
            || RenamedColumns.Count > 0;

        /// <summary>
        /// Indicates whether the diff contains table or column drops that can destroy data.
        /// </summary>
        public bool HasDestructiveChanges => DroppedTables.Count > 0 || DroppedColumns.Count > 0;

        /// <summary>
        /// Returns human-readable warnings for destructive operations in this diff.
        /// </summary>
        public IReadOnlyList<string> GetDestructiveChangeWarnings()
        {
            if (!HasDestructiveChanges)
                return Array.Empty<string>();

            var warnings = new List<string>(DroppedTables.Count + DroppedColumns.Count);
            foreach (var table in DroppedTables)
            {
                warnings.Add($"Drop table '{table.Name}' will remove all data in that table. If this is a rename, replace the generated drop/create with a provider-specific rename operation.");
            }

            foreach (var (table, column) in DroppedColumns)
            {
                var candidate = AddedColumns
                    .Where(x => string.Equals(x.Table.Name, table.Name, StringComparison.OrdinalIgnoreCase))
                    .Select(x => x.Column)
                    .FirstOrDefault(x =>
                        string.Equals(x.ClrType, column.ClrType, StringComparison.OrdinalIgnoreCase) &&
                        x.Precision == column.Precision &&
                        x.Scale == column.Scale &&
                        x.IsNullable == column.IsNullable);

                var message = $"Drop column '{table.Name}.{column.Name}' will remove data in that column.";
                if (candidate != null)
                    message += $" Possible rename candidate: '{table.Name}.{candidate.Name}'.";
                message += " If this is a rename, replace the generated drop/add with a provider-specific rename operation.";
                warnings.Add(message);
            }

            return warnings;
        }
    }

    /// <summary>
    /// Provides methods for computing differences between two schema snapshots.
    /// </summary>
    public static class SchemaDiffer
    {
        /// <summary>
        /// Computes the difference between two schema snapshots.
        /// </summary>
        /// <param name="oldSnapshot">The snapshot representing the current database schema.</param>
        /// <param name="newSnapshot">The snapshot representing the desired schema.</param>
        /// <returns>A <see cref="SchemaDiff"/> describing the operations required to transform the schema.</returns>
        public static SchemaDiff Diff(SchemaSnapshot oldSnapshot, SchemaSnapshot newSnapshot)
        {
            ArgumentNullException.ThrowIfNull(oldSnapshot);
            ArgumentNullException.ThrowIfNull(newSnapshot);

            if (oldSnapshot.Tables is null)
                throw new ArgumentException("oldSnapshot.Tables must not be null.", nameof(oldSnapshot));
            if (newSnapshot.Tables is null)
                throw new ArgumentException("newSnapshot.Tables must not be null.", nameof(newSnapshot));

            // Validate FK arrays on both snapshots before starting the diff.
            foreach (var t in oldSnapshot.Tables)
                foreach (var fk in t.ForeignKeys)
                    ValidateFkSchema(fk, t.Name);
            foreach (var t in newSnapshot.Tables)
                foreach (var fk in t.ForeignKeys)
                    ValidateFkSchema(fk, t.Name);

            // Build O(1) lookup dictionaries to avoid O(n²) scans inside the loops.
            // Use last-wins for duplicate table names to match the pre-existing FirstOrDefault behaviour.
            var oldByName = new Dictionary<string, TableSchema>(StringComparer.OrdinalIgnoreCase);
            foreach (var t in oldSnapshot.Tables) oldByName[t.Name] = t;
            var newByName = new Dictionary<string, TableSchema>(StringComparer.OrdinalIgnoreCase);
            foreach (var t in newSnapshot.Tables) newByName[t.Name] = t;

            var diff = new SchemaDiff();
            foreach (var newTable in newSnapshot.Tables)
            {
                if (!oldByName.TryGetValue(newTable.Name, out var oldTable))
                {
                    diff.AddedTables.Add(newTable);
                    continue;
                }

                // Build O(1) column lookup for the old table.
                var oldColByName = oldTable.Columns.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);

                // Collect the set of old column names that are consumed by a [RenameColumn] declaration
                // so that the dropped-column loop skips them (they are not truly dropped).
                var renamedOldNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                foreach (var col in newTable.Columns)
                {
                    // Check for [RenameColumn("OldName")] — prefer rename over drop+add when the
                    // referenced old column exists in the old snapshot.
                    if (col.PreviousName != null && oldColByName.ContainsKey(col.PreviousName))
                    {
                        diff.RenamedColumns.Add((newTable, col.PreviousName, col));
                        renamedOldNames.Add(col.PreviousName);
                        // The new column name is now "in use"; skip add/alter detection for it.
                        continue;
                    }

                    if (!oldColByName.TryGetValue(col.Name, out var oldCol))
                        diff.AddedColumns.Add((newTable, col));
                    else if (!string.Equals(oldCol.ClrType, col.ClrType, StringComparison.OrdinalIgnoreCase)
                        || oldCol.Precision != col.Precision
                        || oldCol.Scale != col.Scale
                        || oldCol.IsNullable != col.IsNullable
                        || oldCol.IsPrimaryKey != col.IsPrimaryKey
                        || oldCol.IsUnique != col.IsUnique
                        || !string.Equals(oldCol.IndexName, col.IndexName, StringComparison.OrdinalIgnoreCase)
                        || !string.Equals(oldCol.DefaultValue, col.DefaultValue, StringComparison.OrdinalIgnoreCase)  // OrdinalIgnoreCase: SQL keyword case differences like CURRENT_TIMESTAMP vs current_timestamp must not trigger spurious migrations
                        || oldCol.IsIdentity != col.IsIdentity)  // detect identity changes
                        diff.AlteredColumns.Add((newTable, col, oldCol));
                }

                // Build O(1) new-column lookup for dropped-column detection.
                var newColByName = newTable.Columns.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);

                // Detect dropped columns — columns present in old but not in new.
                // Use oldTable (not newTable) because the column existed in the OLD schema.
                // Skip old columns that were renamed via [RenameColumn] (they are not truly dropped).
                foreach (var oldCol in oldTable.Columns)
                {
                    if (!newColByName.ContainsKey(oldCol.Name) && !renamedOldNames.Contains(oldCol.Name))
                        diff.DroppedColumns.Add((oldTable, oldCol));
                }

                // Detect index changes — compare named indexes between old and new
                var oldIndexes = BuildIndexMap(oldTable);
                var newIndexes = BuildIndexMap(newTable);

                foreach (var (name, (isUnique, cols, descending, included)) in newIndexes)
                {
                    if (!oldIndexes.ContainsKey(name))
                    {
                        diff.AddedIndexes.Add((newTable, name, isUnique, cols, descending));
                    }
                    else
                    {
                        // Index exists in both — check for definition changes
                        var (oldIsUnique, oldCols, oldDescending, oldIncluded) = oldIndexes[name];
                        // Compare column lists in declared order; (A,B) and (B,A) are semantically distinct indexes.
                        var colsChanged = !oldCols.SequenceEqual(cols, StringComparer.OrdinalIgnoreCase);
                        var directionChanged = !oldDescending.SequenceEqual(descending);
                        var includedChanged = !oldIncluded.SequenceEqual(included, StringComparer.OrdinalIgnoreCase);
                        if (oldIsUnique != isUnique || colsChanged || directionChanged || includedChanged)
                        {
                            // Use oldTable for DroppedIndexes (the index existed on the OLD table).
                            diff.DroppedIndexes.Add((oldTable, name));
                            diff.AddedIndexes.Add((newTable, name, isUnique, cols, descending));
                        }
                    }
                }

                foreach (var (name, _) in oldIndexes)
                {
                    if (!newIndexes.ContainsKey(name))
                        // Use oldTable for DroppedIndexes (the index existed on the OLD table).
                        diff.DroppedIndexes.Add((oldTable, name));
                }

                // Detect FK constraint changes — add/drop/alter foreign keys
                var oldFks = BuildFkMap(oldTable);
                var newFks = BuildFkMap(newTable);

                foreach (var (name, newFk) in newFks)
                {
                    if (!oldFks.TryGetValue(name, out var oldFk))
                    {
                        diff.AddedForeignKeys.Add((newTable, newFk));
                    }
                    else if (!FkEqual(oldFk, newFk))
                    {
                        // Definition changed: drop old, add new
                        diff.DroppedForeignKeys.Add((newTable, oldFk));
                        diff.AddedForeignKeys.Add((newTable, newFk));
                    }
                }
                foreach (var (name, oldFk) in oldFks)
                {
                    if (!newFks.ContainsKey(name))
                        diff.DroppedForeignKeys.Add((newTable, oldFk));
                }
            }

            // Detect dropped tables — tables present in old but not in new.
            // Use the pre-built newByName dictionary for O(1) lookup.
            foreach (var oldTable in oldSnapshot.Tables)
            {
                if (!newByName.ContainsKey(oldTable.Name))
                    diff.DroppedTables.Add(oldTable);
            }

            return diff;
        }

        /// <summary>
        /// Validates that a <see cref="ForeignKeySchema"/>'s column arrays are non-empty and
        /// that the principal table name is non-blank. Throws <see cref="ArgumentException"/> on violation.
        /// </summary>
        private static void ValidateFkSchema(ForeignKeySchema fk, string owningTableName)
        {
            if (fk.DependentColumns == null || fk.DependentColumns.Length == 0)
                throw new ArgumentException(
                    $"FK '{fk.ConstraintName}' on table '{owningTableName}' has no DependentColumns.");
            if (fk.PrincipalColumns == null || fk.PrincipalColumns.Length == 0)
                throw new ArgumentException(
                    $"FK '{fk.ConstraintName}' on table '{owningTableName}' has no PrincipalColumns.");
            if (string.IsNullOrWhiteSpace(fk.PrincipalTable))
                throw new ArgumentException(
                    $"FK '{fk.ConstraintName}' on table '{owningTableName}' has no PrincipalTable.");
        }

        /// <summary>
        /// Builds a name-keyed map of FK constraints for a table.
        /// Duplicate constraint names are detected and cause an <see cref="InvalidOperationException"/>.
        /// </summary>
        private static Dictionary<string, ForeignKeySchema> BuildFkMap(TableSchema table)
        {
            var map = new Dictionary<string, ForeignKeySchema>(table.ForeignKeys.Count, StringComparer.OrdinalIgnoreCase);
            foreach (var fk in table.ForeignKeys)
            {
                if (!map.TryAdd(fk.ConstraintName, fk))
                    throw new InvalidOperationException(
                        $"Duplicate FK constraint name '{fk.ConstraintName}' on table '{table.Name}'.");
            }
            return map;
        }

        /// <summary>
        /// Returns true when two FK schemas represent the same constraint definition.
        /// Column order within each array is significant.
        /// </summary>
        /// <remarks>
        /// <see cref="ForeignKeySchema.ConstraintName"/> is intentionally NOT compared inside this method.
        /// FK rename detection is handled by the key-based lookup in <see cref="BuildFkMap"/>:
        /// that method indexes FKs by <c>ConstraintName</c>, so a pure FK rename (same
        /// columns/tables/actions, different name) produces two distinct dictionary entries —
        /// the old name is "not in new" (DroppedForeignKeys) and the new name is "not in old"
        /// (AddedForeignKeys). <c>FkEqual</c> is only called when the <em>same</em>
        /// <c>ConstraintName</c> key is found in both maps, so the names are equal by definition.
        /// </remarks>
        private static bool FkEqual(ForeignKeySchema a, ForeignKeySchema b) =>
            string.Equals(a.PrincipalTable, b.PrincipalTable, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(a.OnDelete, b.OnDelete, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(a.OnUpdate, b.OnUpdate, StringComparison.OrdinalIgnoreCase) &&
            a.DependentColumns.Length == b.DependentColumns.Length &&
            a.DependentColumns.Zip(b.DependentColumns, (x, y) =>
                string.Equals(x, y, StringComparison.OrdinalIgnoreCase)).All(eq => eq) &&
            a.PrincipalColumns.Length == b.PrincipalColumns.Length &&
            a.PrincipalColumns.Zip(b.PrincipalColumns, (x, y) =>
                string.Equals(x, y, StringComparison.OrdinalIgnoreCase)).All(eq => eq);

        internal static IReadOnlyList<(string IndexName, bool IsUnique, string[] ColumnNames, bool[] Descending, string[] IncludedColumnNames)> GetExplicitIndexes(TableSchema table)
            => BuildIndexMap(table)
                .Where(static index => !index.Key.StartsWith("__PK__", StringComparison.Ordinal) &&
                                       !index.Key.StartsWith("__UQ__", StringComparison.Ordinal))
                .Select(static index => (index.Key, index.Value.IsUnique, index.Value.ColumnNames, index.Value.Descending, index.Value.IncludedColumnNames))
                .ToArray();

        /// <summary>
        /// Builds a map of index name to (IsUnique, column names[]) from the columns of a table.
        /// Only columns that carry an <see cref="ColumnSchema.IndexName"/> or are PK/Unique are included.
        /// </summary>
        private static Dictionary<string, (bool IsUnique, string[] ColumnNames, bool[] Descending, string[] IncludedColumnNames)> BuildIndexMap(TableSchema table)
        {
            var intermediate = new Dictionary<string, (bool IsUnique, List<(int? Order, int Sequence, string Name, bool IsDescending)> Columns, List<(int Sequence, string Name)> IncludedColumns)>(StringComparer.OrdinalIgnoreCase);
            var sequence = 0;
            foreach (var col in table.Columns)
            {
                if (col.Indexes.Count > 0)
                {
                    foreach (var index in col.Indexes)
                    {
                        if (string.IsNullOrWhiteSpace(index.Name))
                            continue;
                        if (!intermediate.TryGetValue(index.Name, out var explicitEntry))
                            explicitEntry = (index.IsUnique, new List<(int? Order, int Sequence, string Name, bool IsDescending)>(), new List<(int Sequence, string Name)>());
                        explicitEntry.IsUnique = explicitEntry.IsUnique || index.IsUnique;
                        if (index.IsIncluded)
                            explicitEntry.IncludedColumns.Add((sequence++, col.Name));
                        else
                            explicitEntry.Columns.Add((index.Order, sequence++, col.Name, index.IsDescending));
                        intermediate[index.Name] = explicitEntry;
                    }
                }

                // Include columns that have an explicit IndexName, or are unique/PK (implicit constraint).
                // This ensures that changes to IsPrimaryKey or IsUnique flow through AddedIndexes/DroppedIndexes.
                string? indexKey = col.Indexes.Count > 0 ? null : col.IndexName;
                if (indexKey == null)
                {
                    if (col.IsPrimaryKey)
                        indexKey = $"__PK__{col.Name}";
                    else if (col.IsUnique)
                        indexKey = $"__UQ__{col.Name}";
                    else
                        continue;
                }
                if (!intermediate.TryGetValue(indexKey, out var entry))
                    entry = (col.IsUnique || col.IsPrimaryKey, new List<(int? Order, int Sequence, string Name, bool IsDescending)>(), new List<(int Sequence, string Name)>());
                entry.IsUnique = entry.IsUnique || col.IsUnique || col.IsPrimaryKey;
                entry.Columns.Add((col.IndexOrder, sequence++, col.Name, false));
                intermediate[indexKey] = entry;
            }

            var map = new Dictionary<string, (bool IsUnique, string[] ColumnNames, bool[] Descending, string[] IncludedColumnNames)>(intermediate.Count, StringComparer.OrdinalIgnoreCase);
            foreach (var (key, (isUnique, columns, includedColumnsRaw)) in intermediate)
            {
                var orderedColumns = columns
                    .OrderBy(static column => column.Order ?? int.MaxValue)
                    .ThenBy(static column => column.Sequence)
                    .ToArray();
                if (orderedColumns.Length == 0)
                    continue;

                var includedColumns = includedColumnsRaw
                    .OrderBy(static column => column.Sequence)
                    .Select(static column => column.Name)
                    .ToArray();
                map[key] = (isUnique, orderedColumns.Select(static column => column.Name).ToArray(), orderedColumns.Select(static column => column.IsDescending).ToArray(), includedColumns);
            }
            return map;
        }

    }
}
