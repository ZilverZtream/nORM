using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using System.Linq.Expressions;
using System.Text;

#nullable enable

namespace nORM.Mapping
{
    /// <summary>
    /// Describes how a CLR type maps to a database table including column and relationship metadata.
    /// </summary>
    // Mapping construction reflects over the entity type and builds column accessor
    // delegates, so the constructor carries RequiresDynamicCode/RequiresUnreferencedCode.
    // The static expression-analysis helpers and materialized instance state are safe to
    // use from any context once a mapping exists, so the class itself is not annotated.
    public sealed class TableMapping
    {
        /// <summary>Gets the CLR type represented by this mapping.</summary>
        public Type Type { get; }

        /// <summary>Gets the escaped table name used in SQL statements.</summary>
        public string EscTable { get; set; }

        /// <summary>Gets all mapped columns.</summary>
        public Column[] Columns { get; }

        /// <summary>Gets a lookup of columns by logical model property name.</summary>
        public Dictionary<string, Column> ColumnsByName { get; }

        /// <summary>Gets the columns that form the primary key.</summary>
        public Column[] KeyColumns { get; }

        /// <summary>Gets whether this mapping is query-only and rejects writes.</summary>
        public bool IsReadOnly { get; }

        /// <summary>Gets whether this mapping is keyless (a query type): never tracked and not savable.</summary>
        public bool IsKeyless { get; }

        /// <summary>Gets the timestamp column used for concurrency, if any.</summary>
        public Column? TimestampColumn { get; }

        /// <summary>
        /// True when nORM writes a fresh <see cref="TimestampColumn"/> value on each UPDATE (providers
        /// without a native rowversion). When true the token column is part of <see cref="UpdateColumns"/>.
        /// </summary>
        public bool ClientManagedConcurrencyToken { get; }

        /// <summary>
        /// True for a concurrency-token CLR type nORM can auto-manage (stamp a fresh value each UPDATE) via
        /// <see cref="nORM.Core.ConcurrencyTokenGenerator"/> on a provider without a native rowversion. Kept in
        /// lock-step with that generator's supported types so the two never disagree.
        /// </summary>
        private static bool IsAutoManageableTokenType(Type t)
            => t == typeof(byte[]) || t == typeof(Guid)
               || t == typeof(long) || t == typeof(ulong)
               || t == typeof(int) || t == typeof(uint);

        /// <summary>Gets the tenant discriminator column, if multi-tenancy is enabled.</summary>
        public Column? TenantColumn { get; }

        /// <summary>Gets the set of columns included in insert statements.</summary>
        public Column[] InsertColumns { get; }

        /// <summary>
        /// The single store-generated-key CONVENTION column (EF Core parity), or <c>null</c> when the entity
        /// has none. Set only when the provider supports store-generating it (<see
        /// cref="nORM.Providers.DatabaseProvider.SupportsConventionKeyStoreGeneration"/>), so on other
        /// providers this stays null and the write path / change tracker behave exactly as before. When
        /// present, an INSERT of an entity whose key is default omits this column and reads back the
        /// generated value; a non-default value is inserted as-is (honored).
        /// </summary>
        internal Column? ConventionGeneratedKeyColumn { get; }

        /// <summary>
        /// <see cref="InsertColumns"/> with the <see cref="ConventionGeneratedKeyColumn"/> removed — the
        /// column set for the default-value (store-generated) insert run. Equals <see cref="InsertColumns"/>
        /// when there is no convention key.
        /// </summary>
        internal Column[] InsertColumnsWithoutConventionKey { get; }

        /// <summary>Gets the set of columns included in update statements.</summary>
        public Column[] UpdateColumns { get; }

        /// <summary>Gets the unescaped table name.</summary>
        public string TableName { get; }

        /// <summary>Gets the relationships to dependent entities.</summary>
        public Dictionary<string, Relation> Relations { get; } = new();

        /// <summary>Gets the database provider associated with this mapping.</summary>
        public DatabaseProvider Provider { get; }

        /// <summary>Gets the discriminator column used for TPH inheritance, if any.</summary>
        public Column? DiscriminatorColumn { get; private set; } = null;

        /// <summary>
        /// For a TPH derived type, the fixed discriminator value that identifies this subtype
        /// (from <see cref="DiscriminatorValueAttribute"/>); <c>null</c> for base or non-TPH types.
        /// </summary>
        public object? DiscriminatorValue { get; private set; }

        /// <summary>
        /// Stamps the discriminator column on a derived TPH entity with this subtype's fixed value
        /// before it is written, so an inserted <c>Car</c> persists as a <c>Car</c> and round-trips
        /// to the correct subtype. No-op for base or non-TPH mappings.
        /// </summary>
        internal void ApplyDiscriminator(object entity)
        {
            if (DiscriminatorValue != null && DiscriminatorColumn != null)
                DiscriminatorColumn.Setter(entity, DiscriminatorValue);
        }

        /// <summary>Gets mappings for derived types in TPH inheritance scenarios.</summary>
        public Dictionary<object, TableMapping> TphMappings { get; } = new();

        /// <summary>Gets a fingerprint hash of all converter configurations for cache differentiation.</summary>
        public int ConverterFingerprint { get; private set; }

        /// <summary>Gets a fingerprint hash of shadow property columns for cache differentiation.</summary>
        public int ShadowFingerprint { get; private set; }

        /// <summary>Gets the owned collection mappings for this entity type.</summary>
        public List<OwnedCollectionMapping> OwnedCollections { get; } = new();

        private readonly PropertyInfo[] _referenceNavigations;

        /// <summary>
        /// Writable entity-typed properties that reach another table through a scalar FK
        /// (reference navigations, e.g. <c>Order.Customer</c>). Excludes mapped columns
        /// (a value converter can map a class-typed property to a provider value),
        /// collections, and owned navigations, whose values live in the owner's own
        /// table. Discovery is shape-only and happens in the constructor; FK resolution
        /// happens per use because it needs the principal's mapping.
        /// </summary>
        internal PropertyInfo[] ReferenceNavigations => _referenceNavigations;

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Reflects over entity properties; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Reflects over entity properties; trimming may remove the required members.")]
        private PropertyInfo[] DiscoverReferenceNavigations()
        {
            List<PropertyInfo>? list = null;
            foreach (var prop in Type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (!prop.CanRead || !prop.CanWrite)
                    continue;
                var propertyType = prop.PropertyType;
                if (!propertyType.IsClass || propertyType == typeof(string) || propertyType == typeof(object))
                    continue;
                if (typeof(System.Collections.IEnumerable).IsAssignableFrom(propertyType))
                    continue;
                if (ColumnsByName.ContainsKey(prop.Name))
                    continue;
                var isOwned = false;
                if (_fluentConfig != null)
                {
                    foreach (var owned in _fluentConfig.OwnedNavigations)
                    {
                        if (owned.Key.Name == prop.Name)
                        {
                            isOwned = true;
                            break;
                        }
                    }
                }
                if (isOwned)
                    continue;
                (list ??= new List<PropertyInfo>()).Add(prop);
            }
            return list?.ToArray() ?? Array.Empty<PropertyInfo>();
        }

        /// <summary>Gets the many-to-many join table mappings for this entity type.</summary>
        public List<JoinTableMapping> ManyToManyJoins { get; } = new();

        /// <summary>Gets a stable key segment for provider-level SQL shape caches.</summary>
        internal readonly string SqlShapeKey;

        private readonly IEntityTypeConfiguration? _fluentConfig;

        /// <summary>Gets the fluent configuration used to create this mapping, if any.</summary>
        internal IEntityTypeConfiguration? FluentConfiguration => _fluentConfig;

        // Cache derived type discovery to avoid an expensive AppDomain.GetAssemblies() scan on every mapping
        // (scanning all assemblies can take 100–1000 ms+ in large applications).
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, List<Type>> _derivedTypesCache = new();

        /// <summary>
        /// Creates a new <see cref="TableMapping"/> for the specified type and provider.
        /// </summary>
        /// <param name="t">CLR type being mapped.</param>
        /// <param name="p">Database provider used for identifier escaping.</param>
        /// <param name="ctx">Context used to resolve related mappings.</param>
        /// <param name="fluentConfig">Optional configuration overrides.</param>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("TableMapping construction builds column accessor delegates via reflection; not NativeAOT-compatible.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("TableMapping construction reflects over entity properties; trimming may remove the required members.")]
        public TableMapping(Type t, DatabaseProvider p, DbContext ctx, IEntityTypeConfiguration? fluentConfig)
        {
            Type = t;
            Provider = p;
            _fluentConfig = fluentConfig;

            // TPH derived type: it carries [DiscriminatorValue] and shares the table of the ancestor
            // that declares [DiscriminatorColumn]. Resolve that root so writes target the shared table
            // rather than a non-existent table named after the subtype. The root's table name is
            // computed directly from its type/attribute — never via GetMapping(root), which would
            // recurse into the still-constructing root through its TphMappings discovery loop.
            var discriminatorValueAttr = t.GetCustomAttribute<DiscriminatorValueAttribute>(inherit: false);
            Type? tphRoot = null;
            if (discriminatorValueAttr != null)
            {
                for (var anc = t.BaseType; anc != null && anc != typeof(object); anc = anc.BaseType)
                {
                    if (anc.GetCustomAttribute<DiscriminatorColumnAttribute>(inherit: false) != null)
                    {
                        tphRoot = anc;
                        break;
                    }
                }
            }

            var splitAttr = t.GetCustomAttribute<TableSplitAttribute>();
            var splitType = fluentConfig?.TableSplitWith ?? splitAttr?.PrincipalType;
            if (splitType != null)
            {
                var principal = ctx.GetMapping(splitType);
                EscTable = principal.EscTable;
                TableName = principal.TableName;
            }
            else if (tphRoot != null)
            {
                var rootTableAttr = tphRoot.GetCustomAttribute<TableAttribute>();
                TableName = GetTableName(tphRoot, rootTableAttr);
                EscTable = rootTableAttr is not null
                    ? IdentifierEscaping.EscapeTable(p, rootTableAttr.Name, rootTableAttr.Schema)
                    : p.Escape(TableName);
            }
            else
            {
                var tableAttr = t.GetCustomAttribute<TableAttribute>();
                if (fluentConfig?.TableName is { } fluentTableName)
                {
                    EscTable = IdentifierEscaping.EscapeTable(p, fluentTableName, fluentConfig.SchemaName);
                    TableName = FormatTableName(fluentTableName, fluentConfig.SchemaName);
                }
                else
                {
                    TableName = GetTableName(t, tableAttr);
                    EscTable = tableAttr is not null
                        ? IdentifierEscaping.EscapeTable(p, tableAttr.Name, tableAttr.Schema)
                        : p.Escape(TableName);
                }
            }

            var cols = ColumnMappingCache.GetCachedColumns(t, p, fluentConfig).ToList();

            if (fluentConfig?.ShadowProperties.Count > 0)
            {
                foreach (var sp in fluentConfig.ShadowProperties)
                {
                    cols.Add(new Column(sp.Key, sp.Value.ClrType, t, p, sp.Value.ColumnName));
                }
            }
            AddOwnedNavigationShadowColumns(cols, t, p, fluentConfig);

            if (tphRoot != null)
            {
                // Derived TPH type: bind the inherited discriminator column and the fixed value that
                // identifies this subtype, so ApplyDiscriminator stamps it before every insert.
                var rootDiscAttr = tphRoot.GetCustomAttribute<DiscriminatorColumnAttribute>(inherit: false)!;
                DiscriminatorColumn = cols.FirstOrDefault(c => string.Equals(c.Prop.Name, rootDiscAttr.PropertyName, StringComparison.OrdinalIgnoreCase));
                DiscriminatorValue = discriminatorValueAttr!.Value;
            }

            var discriminatorAttr = t.GetCustomAttribute<DiscriminatorColumnAttribute>();
            if (discriminatorAttr != null)
            {
                DiscriminatorColumn = cols.FirstOrDefault(c => string.Equals(c.Prop.Name, discriminatorAttr.PropertyName, StringComparison.OrdinalIgnoreCase));

                // Use cached derived type discovery instead of scanning all assemblies on each call.
                var derivedTypes = _derivedTypesCache.GetOrAdd(t, baseType =>
                {
                    // Only scan assemblies once per base type, then cache forever
                    var types = new List<Type>();
                    foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
                    {
                        try
                        {
                            // Filter types efficiently: only those with DiscriminatorValueAttribute and matching base
                            var assemblyTypes = assembly.GetTypes();
                            foreach (var tp in assemblyTypes)
                            {
                                if (tp.BaseType == baseType && tp.GetCustomAttribute<DiscriminatorValueAttribute>() != null)
                                {
                                    types.Add(tp);
                                }
                            }
                        }
                        catch (ReflectionTypeLoadException)
                        {
                            // Skip assemblies that can't be fully loaded (e.g., missing dependencies)
                            // This is safer than crashing on dynamic assemblies or partially loaded modules
                        }
                    }
                    return types;
                });

                foreach (var dt in derivedTypes)
                {
                    var value = dt.GetCustomAttribute<DiscriminatorValueAttribute>()!.Value;
                    var map = ctx.GetMapping(dt);
                    TphMappings[value] = map;
                    foreach (var dc in map.Columns)
                    {
                        if (!cols.Any(c => string.Equals(c.Prop.Name, dc.Prop.Name, StringComparison.Ordinal)))
                            cols.Add(dc);
                    }
                }
            }

            IsReadOnly = (fluentConfig?.IsReadOnly ?? false)
                || t.GetCustomAttribute<ReadOnlyEntityAttribute>(inherit: true) != null;

            IsKeyless = fluentConfig?.IsKeyless ?? false;

            Columns = cols.ToArray();
            ColumnsByName = BuildColumnsByName(Columns, t);

            // EF-parity key convention: when no explicit key is configured ([Key]/HasKey), a
            // property named "Id" - then "<Type>Id" - becomes the primary key, matching the
            // assembly-driven snapshot builder's convention so the mapper and design-time model
            // agree. Explicit configuration always wins; prefixed owned/shadow columns never
            // match (their PropName carries the prefix). A keyless entity (HasNoKey) opts out of the
            // convention entirely so it stays keyless even when an "Id" property is present.
            if (!IsKeyless && !Columns.Any(c => c.IsKey))
            {
                var conventionKey = Columns.FirstOrDefault(c => string.Equals(c.PropName, "Id", StringComparison.Ordinal))
                    ?? Columns.FirstOrDefault(c => string.Equals(c.PropName, t.Name + "Id", StringComparison.Ordinal));
                conventionKey?.MarkAsConventionKey();
            }

            KeyColumns = Columns.Where(c => c.IsKey).ToArray();
            // Store-generated-key convention (EF Core parity): a SINGLE-column integer primary key with no
            // explicit value-generation config is store-generated when its value is default (0) and honored
            // when non-default. Composite keys and explicitly-configured keys ([DatabaseGenerated]/
            // ValueGeneratedOnAdd/Never) are excluded, so an opt-out stays client-set. Gated on the provider
            // actually being able to store-generate it (SQLite: rowid alias) so the mark — and therefore the
            // write-path and change-tracker behavior keyed off IsConventionGeneratedKey — is a no-op on
            // providers that do not yet support it. See honor_nonzero_key_convention_plan.
            Column? conventionGeneratedKey = null;
            if (KeyColumns.Length == 1
                && !KeyColumns[0].IsDbGenerated
                && !KeyColumns[0].ValueGenerationExplicitlyConfigured
                && Column.IsConventionStoreGeneratedKeyType(KeyColumns[0].Prop.PropertyType)
                && p.SupportsConventionKeyStoreGeneration)
            {
                KeyColumns[0].MarkConventionGeneratedKey();
                conventionGeneratedKey = KeyColumns[0];
            }
            ConventionGeneratedKeyColumn = conventionGeneratedKey;
            TimestampColumn = Columns.FirstOrDefault(c => c.IsTimestamp);
            TenantColumn = Columns.FirstOrDefault(c => c.PropName == ctx.Options.TenantColumnName);
            InsertColumns = Columns.Where(c => !c.IsDbGenerated).ToArray();
            InsertColumnsWithoutConventionKey = conventionGeneratedKey != null
                ? InsertColumns.Where(c => !ReferenceEquals(c, conventionGeneratedKey)).ToArray()
                : InsertColumns;
            // On providers without a native rowversion (SQLite/PostgreSQL/MySQL), nORM writes a fresh
            // [Timestamp] value on every UPDATE (appended to the SET clause by the batched write path)
            // so stale concurrent writes are detected. SQL Server's ROWVERSION is DB-generated and read
            // back from the OUTPUT clause instead. UpdateColumns stays token-free (the token is bound
            // separately) so provider SQL that names params by column don't collide on the token.
            //
            // Every auto-manageable rowversion type (byte[], Guid, and the integer widths) is nORM-managed on
            // a provider without a native rowversion: nORM stamps a fresh value in SET and compares the
            // ORIGINAL snapshot in WHERE, so a stale writer is rejected. Previously only byte[] was managed,
            // so a Guid/int/long token was compared in WHERE but never mutated — the stored value stayed
            // constant and every stale writer matched, a SILENT lost update. A string (or other) token stays
            // compare-only: nORM emits the snapshot comparison in WHERE, and the application or a database
            // trigger owns advancing the token value.
            ClientManagedConcurrencyToken = TimestampColumn != null && !p.SupportsNativeRowVersion
                && IsAutoManageableTokenType(Nullable.GetUnderlyingType(TimestampColumn.Prop.PropertyType) ?? TimestampColumn.Prop.PropertyType);
            UpdateColumns = Columns.Where(c => !c.IsKey && !c.IsTimestamp && !c.IsDbGenerated).ToArray();

            // Compute converter fingerprint for materializer cache differentiation
            int fp = 0;
            foreach (var col in Columns)
                if (col.Converter != null) fp = HashCode.Combine(fp, col.Name, col.Converter.GetType());
            ConverterFingerprint = fp;

            // Compute shadow fingerprint so that different shadow-property configurations for
            // the same entity type produce distinct materializer cache entries.
            int sfp = Columns.Length; // include total column count
            foreach (var col in Columns)
                if (col.IsShadow) sfp = HashCode.Combine(sfp, col.Name);
            ShadowFingerprint = sfp;
            SqlShapeKey = BuildSqlShapeKey();

            // Shape-only discovery (no related mappings needed), so it can run eagerly here
            // inside the annotated constructor — a lazy property getter would trip the AOT
            // gate's IL2075 on Type.GetProperties from unannotated call paths.
            _referenceNavigations = DiscoverReferenceNavigations();

            // Relationship discovery is deferred to EnsureRelationshipsInitialized so that a
            // mapping is fully present in the context cache BEFORE it resolves any related
            // mapping. Resolving relationships inside the constructor made a self-referential
            // entity (Category→Parent, Employee→Manager) recurse into GetMapping for its own,
            // not-yet-cached type — an unrecoverable StackOverflowException at first use.
        }

        // 0 = pending, 1 = in progress (re-entrant cycle), 2 = complete.
        private volatile int _relInitState;

        /// <summary>
        /// Gets whether relationship discovery has completed for this mapping.
        /// </summary>
        internal bool RelationshipsInitialized => _relInitState == 2;

        /// <summary>
        /// Completes the second construction phase — relationship, owned-collection and
        /// many-to-many discovery — which must run after the mapping is registered in the
        /// context cache so cyclic type graphs (self-references, mutually referencing
        /// entities) resolve against the cached instance instead of recursing.
        /// </summary>
        /// <remarks>
        /// The caller (<see cref="DbContext.GetMapping"/>) holds a context-level, thread-reentrant
        /// lock across the whole discovery cycle. A re-entrant call for a mapping already in
        /// progress on the same cycle returns immediately: its columns are fully built (phase one),
        /// which is all a dependent mapping needs to resolve its foreign keys.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Relationship discovery builds accessor delegates via reflection; not NativeAOT-compatible.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Relationship discovery reflects over entity properties; trimming may remove the required members.")]
        internal void EnsureRelationshipsInitialized(DbContext ctx)
        {
            // Already complete, or in progress higher up the same construction cycle.
            if (_relInitState != 0)
                return;

            _relInitState = 1;
            try
            {
                DiscoverRelations(ctx);
                BuildOwnedCollections(_fluentConfig, Provider);
                BuildManyToManyJoins(_fluentConfig, Provider, ctx);
                _relInitState = 2;
            }
            catch
            {
                // Leave the mapping re-initializable after a transient failure rather than
                // stranding it half-built.
                _relInitState = 0;
                throw;
            }
        }

        private static Dictionary<string, Column> BuildColumnsByName(IEnumerable<Column> columns, Type entityType)
        {
            var byName = new Dictionary<string, Column>(StringComparer.Ordinal);
            foreach (var column in columns)
            {
                if (byName.TryGetValue(column.PropName, out var existing))
                {
                    if (ReferenceEquals(existing, column))
                        continue;

                    throw new NormConfigurationException(string.Format(ErrorMessages.InvalidConfiguration,
                        $"Entity '{entityType.Name}' maps multiple columns to logical property '{column.PropName}' " +
                        $"('{existing.Name}' and '{column.Name}'). Use distinct property or owned-navigation names."));
                }

                byName[column.PropName] = column;
            }

            return byName;
        }

        internal bool TryGetColumnForMemberAccess(MemberExpression member, out Column column)
        {
            if (TryGetMemberAccessPath(member, out var path) && ColumnsByName.TryGetValue(path, out column!))
                return true;

            column = null!;
            return false;
        }

        internal static bool TryGetMemberAccessRoot(MemberExpression member, out ParameterExpression parameter)
        {
            Expression? current = member;
            while (true)
            {
                current = UnwrapConversion(current);
                if (current is MemberExpression nested)
                {
                    current = nested.Expression;
                    continue;
                }

                if (current is ParameterExpression root)
                {
                    parameter = root;
                    return true;
                }

                parameter = null!;
                return false;
            }
        }

        internal static bool TryGetMemberAccessPath(MemberExpression member, out string path)
        {
            var names = new List<string>();
            Expression? current = member;
            while (true)
            {
                current = UnwrapConversion(current);
                if (current is not MemberExpression nested)
                    break;

                names.Add(nested.Member.Name);
                current = nested.Expression;
            }

            if (current is not ParameterExpression || names.Count == 0)
            {
                path = string.Empty;
                return false;
            }

            names.Reverse();
            path = string.Join("_", names);
            return true;
        }

        private static Expression? UnwrapConversion(Expression? expression)
        {
            while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
                expression = convert.Operand;

            return expression;
        }

        private string BuildSqlShapeKey()
        {
            var sb = new StringBuilder();
            AppendSegment(sb, EscTable);
            AppendSegment(sb, ConverterFingerprint.ToString());
            AppendSegment(sb, ShadowFingerprint.ToString());
            AppendSegment(sb, TenantColumn?.Name ?? string.Empty);
            AppendSegment(sb, TimestampColumn?.Name ?? string.Empty);
            sb.Append(Columns.Length).Append('|');
            foreach (var col in Columns)
            {
                AppendSegment(sb, col.PropName);
                AppendSegment(sb, col.Name);
                AppendSegment(sb, col.EscCol);
                sb.Append(col.IsKey ? '1' : '0')
                  .Append(col.IsDbGenerated ? '1' : '0')
                  .Append(col.IsTimestamp ? '1' : '0')
                  .Append(col.IsShadow ? '1' : '0')
                  .Append('|');
            }

            return sb.ToString();
        }

        private static void AppendSegment(StringBuilder sb, string value)
            => sb.Append(value.Length).Append(':').Append(value).Append('|');

        /// <summary>
        /// Matches a mapped column's property against a fluent key / foreign-key selector's property. Reference
        /// equality first, then <see cref="System.Reflection.MemberInfo.HasSameMetadataDefinitionAs"/> so an
        /// INHERITED key/FK (declared on a base type, e.g. a TPH root) still resolves: the selector reflects the
        /// property through the base type (<c>d =&gt; d.OwnerId</c> on a subtype yields the base's PropertyInfo)
        /// while the dependent's column reflects it through the derived type, so their <c>ReflectedType</c>
        /// differs and reference equality fails. Same metadata definition means the same property regardless of
        /// how it was reflected.
        /// </summary>
        private static bool PropertyMatches(System.Reflection.PropertyInfo columnProperty, System.Reflection.PropertyInfo selectorProperty)
            => columnProperty == selectorProperty || columnProperty.HasSameMetadataDefinitionAs(selectorProperty);

        /// <summary>
        /// Inspects the entity type and associated configuration to build the collection of
        /// relationships that describe how this entity links to dependents. Both explicitly
        /// configured relationships and convention-based matches are considered.
        /// </summary>
        /// <param name="ctx">The <see cref="DbContext"/> used to resolve related entity mappings.</param>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Mapping construction builds column accessor delegates via reflection; not NativeAOT-compatible.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Mapping construction reflects over entity properties; trimming may remove the required members.")]
        private void DiscoverRelations(DbContext ctx)
        {
            if (_fluentConfig?.Relationships.Count > 0)
            {
                foreach (var rel in _fluentConfig.Relationships)
                {
                    var dependentMap = ctx.GetMapping(rel.DependentType);
                    var principalKeys = rel.PrincipalKeys.Count > 0
                        ? rel.PrincipalKeys.Select(pk => Columns.FirstOrDefault(c => PropertyMatches(c.Prop, pk))
                            ?? throw new NormConfigurationException(string.Format(ErrorMessages.InvalidConfiguration, $"Principal key '{pk.Name}' not found on entity {Type.Name}"))).ToArray()
                        : KeyColumns;
                    var foreignKeys = rel.ForeignKeys.Select(fk => dependentMap.Columns.FirstOrDefault(c => PropertyMatches(c.Prop, fk))
                            ?? throw new NormConfigurationException(string.Format(ErrorMessages.InvalidConfiguration, $"Foreign key '{fk.Name}' not found on entity {dependentMap.Type.Name}"))).ToArray();

                    if (principalKeys.Length == 0 || principalKeys.Length != foreignKeys.Length)
                    {
                        throw new NormConfigurationException(string.Format(ErrorMessages.InvalidConfiguration,
                            $"Relationship '{rel.PrincipalNavigation.Name}' on entity {Type.Name} has {principalKeys.Length} principal key columns but {foreignKeys.Length} foreign key columns."));
                    }

                    Relations[rel.PrincipalNavigation.Name] = new Relation(
                        rel.PrincipalNavigation,
                        rel.DependentType,
                        principalKeys,
                        foreignKeys,
                        rel.CascadeDelete)
                    {
                        OnDelete = rel.OnDelete,
                        OnUpdate = rel.OnUpdate,
                        ConstraintName = rel.ConstraintName
                    };
                }
            }

            foreach (var prop in Type.GetProperties().Where(pr => pr.GetCustomAttribute<NotMappedAttribute>() == null))
            {
                if (Relations.ContainsKey(prop.Name))
                    continue;
                if (TryGetCollectionElementType(prop, out var dependentType))
                {
                    if (HasCollectionNavigationTo(dependentType, Type))
                        continue;

                    var dependentMap = ctx.GetMapping(dependentType);

                    Column? foreignKeyProp = dependentMap.Columns
                        .FirstOrDefault(c => string.Equals(c.ForeignKeyPrincipalTypeName, Type.Name, StringComparison.OrdinalIgnoreCase));

                    // Determine the principal key column: explicit key or convention-based "Id"
                    var principalKey = KeyColumns.Length == 1 ? KeyColumns[0]
                        : Columns.FirstOrDefault(c => string.Equals(c.PropName, "Id", StringComparison.OrdinalIgnoreCase));

                    if (foreignKeyProp == null && principalKey != null)
                    {
                        var fkName = $"{Type.Name}Id";
                        var fkComposite = $"{Type.Name}_{principalKey.PropName}";
                        foreignKeyProp = dependentMap.Columns.FirstOrDefault(c =>
                            string.Equals(c.PropName, fkName, StringComparison.OrdinalIgnoreCase) ||
                            string.Equals(c.PropName, fkComposite, StringComparison.OrdinalIgnoreCase));
                    }

                    if (foreignKeyProp != null && principalKey != null)
                    {
                        Relations[prop.Name] = new Relation(prop, dependentType, principalKey, foreignKeyProp);
                    }
                }
            }
        }

        private static bool TryGetCollectionElementType(PropertyInfo prop, out Type elementType)
        {
            elementType = null!;
            if (!typeof(IEnumerable).IsAssignableFrom(prop.PropertyType) || !prop.PropertyType.IsGenericType)
                return false;

            elementType = prop.PropertyType.GetGenericArguments()[0];
            return elementType != typeof(string);
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Mapping construction builds column accessor delegates via reflection; not NativeAOT-compatible.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Mapping construction reflects over entity properties; trimming may remove the required members.")]
        private static bool HasCollectionNavigationTo(Type type, Type targetType)
            => type.GetProperties()
                .Where(static prop => prop.GetCustomAttribute<NotMappedAttribute>() == null)
                .Any(prop => TryGetCollectionElementType(prop, out var elementType)
                             && elementType == targetType);

        /// <summary>
        /// Sets the value of the primary key on the provided entity for key columns that are
        /// generated by the database (e.g., identity columns).
        /// </summary>
        /// <param name="entity">The entity instance whose key should be set.</param>
        /// <param name="value">The key value produced by the database.</param>
        public void SetPrimaryKey(object entity, object value)
        {
            if (entity is null) throw new ArgumentNullException(nameof(entity));
            if (value is null) throw new ArgumentNullException(nameof(value));

            // Target the DB-generated key, or — for the store-generated convention key's default-value run —
            // the convention key column, so the generated value read back from the INSERT lands on the entity.
            var keyCol = KeyColumns.FirstOrDefault(k => k.IsDbGenerated) ?? ConventionGeneratedKeyColumn;
            if (keyCol != null)
            {
                var targetType = Nullable.GetUnderlyingType(keyCol.Prop.PropertyType) ?? keyCol.Prop.PropertyType;
                var convertedValue = ConvertGeneratedKeyValue(value, targetType);
                keyCol.Setter(entity, convertedValue);
            }
        }

        /// <summary>
        /// Type-aware conversion for DB-generated key values. Handles Guid, byte[], string,
        /// and numeric types. <c>Convert.ChangeType</c> is not used for non-IConvertible types.
        /// </summary>
        private static object ConvertGeneratedKeyValue(object value, Type targetType)
        {
            if (value.GetType() == targetType) return value;

            if (targetType == typeof(Guid))
                return value switch
                {
                    Guid g    => g,
                    string s  => Guid.Parse(s),
                    byte[] b when b.Length == 16 => new Guid(b),
                    _ => Guid.Parse(Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture)
                             ?? throw new InvalidCastException($"Cannot convert {value.GetType().Name} to Guid."))
                };

            if (targetType == typeof(string))
                return Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture)!;

            if (targetType == typeof(byte[]))
                return value is byte[] bytes ? bytes
                    : System.Text.Encoding.UTF8.GetBytes(
                        Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture)!);

            // Numeric and other IConvertible types.
            return Convert.ChangeType(value, targetType, System.Globalization.CultureInfo.InvariantCulture);
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Mapping construction builds column accessor delegates via reflection; not NativeAOT-compatible.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Mapping construction reflects over entity properties; trimming may remove the required members.")]
        private void BuildOwnedCollections(IEntityTypeConfiguration? fluentConfig, DatabaseProvider p)
        {
            if (fluentConfig?.OwnedCollectionNavigations == null) return;
            foreach (var kvp in fluentConfig.OwnedCollectionNavigations)
            {
                var navProp = kvp.Key;
                var nav = kvp.Value;

                var ownedType = nav.OwnedType;

                // Build columns for the owned type (excluding the FK column itself)
                var ownedCols = ColumnMappingCache.GetCachedColumns(ownedType, p, nav.Configuration).ToList();
                if (nav.Configuration?.ShadowProperties.Count > 0)
                {
                    foreach (var sp in nav.Configuration.ShadowProperties)
                    {
                        ownedCols.Add(new Column(sp.Key, sp.Value.ClrType, ownedType, p, sp.Value.ColumnName));
                    }
                }

                var keyColumns = ownedCols.Where(c => c.IsKey).ToArray();

                OwnedCollections.Add(new OwnedCollectionMapping(
                    navProp,
                    ownedType,
                    nav.TableName,
                    nav.SchemaName,
                    nav.ForeignKeyName,
                    ownedCols.ToArray(),
                    keyColumns,
                    nav.Configuration,
                    p));
            }
        }

        private static void AddOwnedNavigationShadowColumns(
            List<Column> columns,
            Type ownerType,
            DatabaseProvider provider,
            IEntityTypeConfiguration? fluentConfig)
        {
            if (fluentConfig is null || fluentConfig.OwnedNavigations.Count == 0)
                return;

            foreach (var (navigationProperty, ownedNavigation) in fluentConfig.OwnedNavigations)
            {
                var shadowProperties = ownedNavigation.Configuration?.ShadowProperties;
                if (shadowProperties is null || shadowProperties.Count == 0)
                    continue;

                foreach (var sp in shadowProperties)
                {
                    var logicalName = navigationProperty.Name + "_" + sp.Key;
                    if (columns.Any(c => string.Equals(c.PropName, logicalName, StringComparison.OrdinalIgnoreCase)))
                        continue;

                    columns.Add(new Column(
                        logicalName,
                        sp.Value.ClrType,
                        ownerType,
                        provider,
                        sp.Value.ColumnName ?? logicalName));
                }
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Mapping construction builds column accessor delegates via reflection; not NativeAOT-compatible.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Mapping construction reflects over entity properties; trimming may remove the required members.")]
        private void BuildManyToManyJoins(IEntityTypeConfiguration? fluentConfig, DatabaseProvider p, DbContext ctx)
        {
            if (fluentConfig?.ManyToManyRelationships == null || fluentConfig.ManyToManyRelationships.Count == 0)
                return;

            foreach (var m2m in fluentConfig.ManyToManyRelationships)
            {
                var leftKeyColumns = ResolveManyToManyKeyColumns(
                    m2m.LeftKeyProperties,
                    Columns,
                    Type,
                    "left");
                if (leftKeyColumns.Length == 0)
                    throw new NormConfigurationException(
                        $"Many-to-many relationship on '{Type.Name}' requires a primary key or explicit left key.");

                if (m2m.LeftFkColumns.Count != leftKeyColumns.Length)
                    throw new NormConfigurationException(
                        $"Many-to-many relationship on '{Type.Name}' declares {m2m.LeftFkColumns.Count} left FK columns " +
                        $"but the entity key has {leftKeyColumns.Length} columns.");

                var rightMapping = ctx.GetMapping(m2m.RelatedType);
                var rightKeyColumns = ResolveManyToManyKeyColumns(
                    m2m.RightKeyProperties,
                    rightMapping.Columns,
                    m2m.RelatedType,
                    "right");
                if (rightKeyColumns.Length == 0)
                    throw new NormConfigurationException(
                        $"Many-to-many relationship on '{Type.Name}' references '{m2m.RelatedType.Name}' which must have a primary key or explicit right key.");

                if (m2m.RightFkColumns.Count != rightKeyColumns.Length)
                    throw new NormConfigurationException(
                        $"Many-to-many relationship on '{Type.Name}' declares {m2m.RightFkColumns.Count} right FK columns " +
                        $"but related entity '{m2m.RelatedType.Name}' has {rightKeyColumns.Length} key columns.");

                // Resolve nav properties
                var leftNavProp = Type.GetProperty(m2m.NavPropertyName)
                    ?? throw new NormConfigurationException(string.Format(ErrorMessages.InvalidConfiguration,
                        $"Navigation property '{m2m.NavPropertyName}' not found on type '{Type.Name}'"));

                System.Reflection.PropertyInfo? rightNavProp = null;
                if (m2m.RelatedNavPropertyName != null)
                    rightNavProp = m2m.RelatedType.GetProperty(m2m.RelatedNavPropertyName);

                ManyToManyJoins.Add(new JoinTableMapping(
                    m2m.JoinTableName,
                    m2m.JoinTableSchema,
                    m2m.LeftFkColumns,
                    m2m.RightFkColumns,
                    Type,
                    m2m.RelatedType,
                    m2m.NavPropertyName,
                    m2m.RelatedNavPropertyName,
                    leftKeyColumns,
                    rightKeyColumns,
                    leftNavProp,
                    rightNavProp,
                    p,
                    m2m.LeftOnDelete,
                    m2m.LeftOnUpdate,
                    m2m.RightOnDelete,
                    m2m.RightOnUpdate));
            }
        }

        private static Column[] ResolveManyToManyKeyColumns(
            IReadOnlyList<PropertyInfo>? keyProperties,
            IReadOnlyList<Column> columns,
            Type entityType,
            string side)
        {
            if (keyProperties is null)
                return columns.Where(c => c.IsKey).ToArray();

            var resolved = new Column[keyProperties.Count];
            for (var i = 0; i < keyProperties.Count; i++)
            {
                var property = keyProperties[i];
                resolved[i] = columns.FirstOrDefault(c => c.Prop == property || c.Prop.Name == property.Name)
                    ?? throw new NormConfigurationException(
                        $"Many-to-many relationship on '{entityType.Name}' declares {side} key property '{property.Name}', " +
                        "but that property is not mapped as a column.");
            }

            return resolved;
        }

        /// <summary>
        /// Represents the mapping of a relationship from the principal entity to a dependent entity type.
        /// </summary>
        /// <param name="NavProp">The navigation property on the principal entity that exposes the related data.</param>
        /// <param name="DependentType">The CLR type of the dependent entity.</param>
        /// <param name="PrincipalKey">The key column on the principal entity used as the relationship principal.</param>
        /// <param name="ForeignKey">The foreign key column on the dependent entity referencing the principal key.</param>
        /// <param name="CascadeDelete">Specifies whether deletes on the principal entity cascade through the tracked object graph.</param>
        public record Relation(PropertyInfo NavProp, Type DependentType, Column PrincipalKey, Column ForeignKey, bool CascadeDelete = true)
        {
            /// <summary>Ordered principal key columns participating in the relationship.</summary>
            public IReadOnlyList<Column> PrincipalKeys { get; init; } = new[] { PrincipalKey };

            /// <summary>Ordered foreign key columns participating in the relationship.</summary>
            public IReadOnlyList<Column> ForeignKeys { get; init; } = new[] { ForeignKey };

            /// <summary>Database referential action emitted for principal deletes.</summary>
            public ReferentialAction OnDelete { get; init; } = CascadeDelete ? ReferentialAction.Cascade : ReferentialAction.NoAction;

            /// <summary>Database referential action emitted for principal key updates.</summary>
            public ReferentialAction OnUpdate { get; init; } = ReferentialAction.NoAction;

            /// <summary>Optional database foreign key constraint name to preserve in migration snapshots.</summary>
            public string? ConstraintName { get; init; }

            /// <summary>Gets whether the relationship spans more than one key column.</summary>
            public bool IsComposite => PrincipalKeys.Count > 1 || ForeignKeys.Count > 1;

            /// <summary>Creates a relationship backed by multiple key columns.</summary>
            public Relation(PropertyInfo navProp, Type dependentType, IReadOnlyList<Column> principalKeys, IReadOnlyList<Column> foreignKeys, bool cascadeDelete = true)
                : this(
                    navProp,
                    dependentType,
                    principalKeys is { Count: > 0 } ? principalKeys[0] : throw new ArgumentException("At least one principal key column is required.", nameof(principalKeys)),
                    foreignKeys is { Count: > 0 } ? foreignKeys[0] : throw new ArgumentException("At least one foreign key column is required.", nameof(foreignKeys)),
                    cascadeDelete)
            {
                if (principalKeys.Count != foreignKeys.Count)
                    throw new ArgumentException("Principal key and foreign key column counts must match.", nameof(foreignKeys));

                PrincipalKeys = principalKeys.ToArray();
                ForeignKeys = foreignKeys.ToArray();
            }
        }

        private static string GetTableName(Type type, TableAttribute? tableAttribute)
        {
            if (tableAttribute is null)
                return type.Name;

            return FormatTableName(tableAttribute.Name, tableAttribute.Schema);
        }

        private static string FormatTableName(string tableName, string? schemaName)
            => string.IsNullOrWhiteSpace(schemaName) ? tableName : schemaName + "." + tableName;
    }

    /// <summary>
    /// Describes an owned collection stored in a child table with a foreign key back to the owner.
    /// </summary>
    public sealed class OwnedCollectionMapping
    {
        /// <summary>The navigation property on the owner entity that holds the collection.</summary>
        public PropertyInfo NavigationProperty { get; }

        /// <summary>CLR element type of the owned collection items.</summary>
        public Type OwnedType { get; }

        /// <summary>Escaped name of the child table that stores owned items.</summary>
        public string EscTable { get; }

        /// <summary>Plain name of the child table.</summary>
        public string TableName { get; }

        /// <summary>Optional schema containing the child table.</summary>
        public string? SchemaName { get; }

        /// <summary>Column name in the child table that holds the FK to the owner's PK.</summary>
        public string ForeignKeyColumn { get; }

        /// <summary>Escaped FK column.</summary>
        public string EscForeignKeyColumn { get; }

        /// <summary>All columns of the owned item (excluding the FK column itself).</summary>
        public Column[] Columns { get; }

        /// <summary>PK columns of the owned item (used for UPDATE/DELETE targeting).</summary>
        public Column[] KeyColumns { get; }

        internal IEntityTypeConfiguration? Configuration { get; }

        /// <summary>Getter that reads the collection from an owner instance.</summary>
        public Func<object, object?> CollectionGetter { get; }

        /// <summary>Setter that assigns the collection on an owner instance.</summary>
        public Action<object, object?> CollectionSetter { get; }

        internal OwnedCollectionMapping(
            PropertyInfo navProp,
            Type ownedType,
            string tableName,
            string? schemaName,
            string foreignKeyColumn,
            Column[] columns,
            Column[] keyColumns,
            IEntityTypeConfiguration? configuration,
            DatabaseProvider provider)
        {
            NavigationProperty = navProp;
            OwnedType = ownedType;
            TableName = tableName;
            SchemaName = schemaName;
            EscTable = IdentifierEscaping.EscapeTable(provider, tableName, schemaName);
            ForeignKeyColumn = foreignKeyColumn;
            EscForeignKeyColumn = IdentifierEscaping.EscapeSingle(provider, foreignKeyColumn);
            Columns = columns;
            KeyColumns = keyColumns;
            Configuration = configuration;

            var entityParam = Expression.Parameter(typeof(object), "e");
            var cast = Expression.Convert(entityParam, navProp.DeclaringType!);
            var getProp = Expression.Property(cast, navProp);
            CollectionGetter = Expression.Lambda<Func<object, object?>>(
                Expression.Convert(getProp, typeof(object)), entityParam).Compile();

            var valueParam = Expression.Parameter(typeof(object), "v");
            var castV = Expression.Convert(valueParam, navProp.PropertyType);
            var setProp = Expression.Call(cast, navProp.GetSetMethod()!, castV);
            CollectionSetter = Expression.Lambda<Action<object, object?>>(
                setProp, entityParam, valueParam).Compile();
        }
    }
}
