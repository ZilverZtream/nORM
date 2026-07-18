using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using nORM.Core;
using nORM.Mapping;

#nullable enable

namespace nORM.Configuration
{
    /// <summary>
    /// Provides a fluent API for configuring how a given entity type is mapped
    /// to database structures such as tables, columns and relationships.
    /// </summary>
    /// <typeparam name="TEntity">The CLR type being configured.</typeparam>
    public partial class EntityTypeBuilder<TEntity> where TEntity : class
    {
        private readonly MappingConfiguration _config = new();
        internal IEntityTypeConfiguration Configuration => _config;

        /// <summary>
        /// Specifies the database table that <typeparamref name="TEntity"/> maps to.
        /// </summary>
        /// <param name="name">The unqualified table name.</param>
        /// <returns>The same <see cref="EntityTypeBuilder{TEntity}"/> instance for chaining.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> is null or whitespace.</exception>
        public EntityTypeBuilder<TEntity> ToTable(string name)
        {
            _config.SetTableName(name);
            return this;
        }

        /// <summary>
        /// Specifies the schema-qualified database table that <typeparamref name="TEntity"/> maps to.
        /// </summary>
        /// <param name="name">The unqualified table name.</param>
        /// <param name="schema">The unescaped database schema containing the table.</param>
        /// <returns>The same <see cref="EntityTypeBuilder{TEntity}"/> instance for chaining.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> is null or whitespace, or <paramref name="schema"/> is whitespace.</exception>
        public EntityTypeBuilder<TEntity> ToTable(string name, string? schema)
        {
            _config.SetTableName(name, schema);
            return this;
        }

        /// <summary>
        /// Maps <typeparamref name="TEntity"/> to the same table as another principal entity type.
        /// </summary>
        /// <typeparam name="TPrincipal">The principal entity that owns the table.</typeparam>
        /// <returns>The same builder instance for fluent chaining.</returns>
        public EntityTypeBuilder<TEntity> SharesTableWith<TPrincipal>()
        {
            _config.SetTableSplit(typeof(TPrincipal));
            return this;
        }

        /// <summary>
        /// Configures one or more properties to constitute the entity's primary key.
        /// </summary>
        /// <param name="keyExpression">Expression selecting the key property or properties.</param>
        /// <returns>The same builder instance for fluent chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="keyExpression"/> is null.</exception>
        /// <exception cref="ArgumentException">
        /// Thrown when the expression selects no properties, or contains a non-property argument.
        /// </exception>
        public EntityTypeBuilder<TEntity> HasKey(Expression<Func<TEntity, object>> keyExpression)
            => HasKey(keyExpression, constraintName: null);

        /// <summary>
        /// Configures one or more properties to constitute the entity's primary key
        /// and preserves the database primary-key constraint name for migration snapshots.
        /// </summary>
        /// <param name="keyExpression">Expression selecting the key property or properties.</param>
        /// <param name="constraintName">Database primary-key constraint name.</param>
        /// <returns>The same builder instance for fluent chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="keyExpression"/> is null.</exception>
        /// <exception cref="ArgumentException">
        /// Thrown when the expression selects no properties, or contains a non-property argument.
        /// </exception>
        public EntityTypeBuilder<TEntity> HasKey(Expression<Func<TEntity, object>> keyExpression, string? constraintName)
        {
            ArgumentNullException.ThrowIfNull(keyExpression);
            if (keyExpression.Body is NewExpression ne)
            {
                if (ne.Arguments.Count == 0)
                    throw new ArgumentException(
                        "HasKey expression must select at least one property. " +
                        "An empty anonymous object 'e => new { }' is not a valid key selector.",
                        nameof(keyExpression));
                foreach (var arg in ne.Arguments)
                {
                    PropertyInfo prop;
                    try { prop = GetProperty(arg); }
                    catch (ArgumentException inner)
                    {
                        throw new ArgumentException(
                            $"HasKey expression contains an invalid argument: {inner.Message}",
                            nameof(keyExpression), inner);
                    }
                    _config.AddKey(prop);
                }
            }
            else
            {
                var prop = GetProperty(keyExpression.Body);
                _config.AddKey(prop);
            }

            _config.SetPrimaryKeyConstraintName(constraintName);
            return this;
        }

        /// <summary>
        /// Marks this entity as query-only. nORM can materialize it from queries,
        /// but insert, update, delete, and tracked SaveChanges writes are rejected.
        /// </summary>
        public EntityTypeBuilder<TEntity> IsReadOnly()
        {
            _config.SetReadOnly();
            return this;
        }

        /// <summary>
        /// Configures this entity as keyless — a query type with no primary key, matching EF Core's
        /// <c>HasNoKey()</c>. Keyless entities are materialized from queries (including database views and
        /// read models) but are never change-tracked and cannot be saved.
        /// </summary>
        public EntityTypeBuilder<TEntity> HasNoKey()
        {
            _config.SetKeyless();
            return this;
        }

        /// <summary>
        /// Configures a table-level CHECK constraint for migration snapshot generation.
        /// The SQL predicate is provider SQL and should not include the outer CHECK keyword.
        /// </summary>
        /// <param name="name">Database constraint name.</param>
        /// <param name="sql">Provider SQL predicate inside the CHECK clause.</param>
        /// <returns>The same builder instance for chaining.</returns>
        /// <exception cref="ArgumentException">Thrown when either argument is null or whitespace.</exception>
        public EntityTypeBuilder<TEntity> HasCheckConstraint(string name, string sql)
        {
            _config.AddCheckConstraint(name, sql);
            return this;
        }

        /// <summary>
        /// Configures a provider-specific index over a SQL expression rather than a mapped property.
        /// </summary>
        /// <param name="name">Database index name.</param>
        /// <param name="expressionSql">Provider SQL expression used as the index key.</param>
        /// <param name="isUnique">Whether the index enforces uniqueness.</param>
        /// <param name="filterSql">Optional provider SQL predicate for a filtered/partial expression index.</param>
        public EntityTypeBuilder<TEntity> HasExpressionIndex(string name, string expressionSql, bool isUnique = false, string? filterSql = null)
        {
            _config.AddExpressionIndex(name, expressionSql, isUnique, filterSql);
            return this;
        }

        /// <summary>
        /// Configures a provider-specific expression index with optional covering columns and PostgreSQL null semantics.
        /// </summary>
        /// <param name="name">Database index name.</param>
        /// <param name="expressionSql">Provider SQL expression used as the index key.</param>
        /// <param name="isUnique">Whether the index enforces uniqueness.</param>
        /// <param name="filterSql">Optional provider SQL predicate for a filtered/partial expression index.</param>
        /// <param name="includedColumnNames">Provider column names included as non-key covering columns where supported.</param>
        /// <param name="nullsNotDistinct">Whether a unique PostgreSQL index treats null values as equal.</param>
        /// <param name="nullSortOrder">Explicit provider null ordering for the expression key.</param>
        public EntityTypeBuilder<TEntity> HasExpressionIndex(
            string name,
            string expressionSql,
            bool isUnique,
            string? filterSql,
            IReadOnlyList<string>? includedColumnNames,
            bool nullsNotDistinct = false,
            IndexNullSortOrder nullSortOrder = IndexNullSortOrder.Default)
        {
            _config.AddExpressionIndex(name, expressionSql, isUnique, filterSql, includedColumnNames, nullsNotDistinct, nullSortOrder);
            return this;
        }

        /// <summary>
        /// Begins configuration for the specified property.
        /// </summary>
        /// <param name="propertyExpression">Expression selecting the property to configure.</param>
        /// <returns>A <see cref="PropertyBuilder"/> for further configuration of the property.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="propertyExpression"/> is null.</exception>
        public PropertyBuilder Property(Expression<Func<TEntity, object>> propertyExpression)
        {
            ArgumentNullException.ThrowIfNull(propertyExpression);
            var prop = GetProperty(propertyExpression);
            return new PropertyBuilder(this, prop);
        }

        /// <summary>
        /// Begins typed configuration for the specified property.
        /// </summary>
        /// <typeparam name="TProperty">The CLR type of the property.</typeparam>
        /// <param name="propertyExpression">Expression selecting the property to configure.</param>
        /// <returns>A <see cref="PropertyBuilder{TProperty}"/> for further configuration including value converters.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="propertyExpression"/> is null.</exception>
        public PropertyBuilder<TProperty> Property<TProperty>(Expression<Func<TEntity, TProperty>> propertyExpression)
        {
            ArgumentNullException.ThrowIfNull(propertyExpression);
            var prop = GetProperty(propertyExpression);
            return new PropertyBuilder<TProperty>(this, prop);
        }

        /// <summary>
        /// Creates a shadow property on the entity type configuration.
        /// </summary>
        /// <typeparam name="TProperty">CLR type of the shadow property.</typeparam>
        /// <param name="name">The name of the shadow property.</param>
        /// <returns>A <see cref="ShadowPropertyBuilder"/> for further configuration.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> is null or whitespace.</exception>
        public ShadowPropertyBuilder Property<TProperty>(string name)
        {
            // Validation delegated to AddShadowProperty
            _config.AddShadowProperty(name, typeof(TProperty));
            return new ShadowPropertyBuilder(this, name);
        }

        /// <summary>
        /// Configures a reference navigation property as an owned entity type.
        /// Owned types share the same table as the owner.
        /// </summary>
        /// <typeparam name="TOwned">The CLR type of the owned entity.</typeparam>
        /// <param name="navigation">Expression selecting the navigation property.</param>
        /// <param name="buildAction">Optional configuration for the owned entity type.</param>
        /// <returns>The same builder instance.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="navigation"/> is null.</exception>
        public EntityTypeBuilder<TEntity> OwnsOne<TOwned>(Expression<Func<TEntity, TOwned>> navigation, Action<EntityTypeBuilder<TOwned>>? buildAction = null) where TOwned : class
        {
            ArgumentNullException.ThrowIfNull(navigation);
            var prop = GetProperty(navigation);
            var builder = new EntityTypeBuilder<TOwned>();
            buildAction?.Invoke(builder);
            _config.AddOwned(prop, builder.Configuration);
            return this;
        }

        /// <summary>
        /// Configures a collection navigation property as an owned collection stored in a separate child table.
        /// </summary>
        /// <typeparam name="TOwned">CLR element type of the owned collection.</typeparam>
        /// <param name="navigation">Expression selecting the collection navigation property.</param>
        /// <param name="tableName">
        /// Name of the child table. Defaults to the owned type name.
        /// Must be supplied explicitly when <typeparamref name="TOwned"/> is a generic type.
        /// </param>
        /// <param name="foreignKey">
        /// FK column name referencing owner's PK. Defaults to owner type name + "Id".
        /// Must be supplied explicitly when <typeparamref name="TEntity"/> is a generic type.
        /// </param>
        /// <param name="buildAction">Optional configuration for the owned element type.</param>
        /// <returns>The same builder instance for chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="navigation"/> is null.</exception>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="tableName"/> is omitted and the owned type is generic,
        /// or when <paramref name="foreignKey"/> is omitted and the owning type is generic.
        /// </exception>
        public EntityTypeBuilder<TEntity> OwnsMany<TOwned>(
            Expression<Func<TEntity, IEnumerable<TOwned>>> navigation,
            string? tableName = null,
            string? foreignKey = null,
            Action<EntityTypeBuilder<TOwned>>? buildAction = null) where TOwned : class
            => OwnsMany(navigation, tableName, foreignKey, schema: null, buildAction);

        /// <summary>
        /// Configures a collection navigation property as an owned collection stored in a separate schema-qualified child table.
        /// </summary>
        /// <typeparam name="TOwned">CLR element type of the owned collection.</typeparam>
        /// <param name="navigation">Expression selecting the collection navigation property.</param>
        /// <param name="tableName">
        /// Name of the child table without schema qualification. Defaults to the owned type name.
        /// Must be supplied explicitly when <typeparamref name="TOwned"/> is a generic type.
        /// </param>
        /// <param name="foreignKey">
        /// FK column name referencing owner's PK. Defaults to owner type name + "Id".
        /// Must be supplied explicitly when <typeparamref name="TEntity"/> is a generic type.
        /// </param>
        /// <param name="schema">Optional schema containing the child table.</param>
        /// <param name="buildAction">Optional configuration for the owned element type.</param>
        /// <returns>The same builder instance for chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="navigation"/> is null.</exception>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="tableName"/> is omitted and the owned type is generic,
        /// when <paramref name="foreignKey"/> is omitted and the owning type is generic, or when
        /// <paramref name="schema"/> is whitespace.
        /// </exception>
        public EntityTypeBuilder<TEntity> OwnsMany<TOwned>(
            Expression<Func<TEntity, IEnumerable<TOwned>>> navigation,
            string? tableName,
            string? foreignKey,
            string? schema,
            Action<EntityTypeBuilder<TOwned>>? buildAction = null) where TOwned : class
        {
            ArgumentNullException.ThrowIfNull(navigation);
            if (schema is not null && string.IsNullOrWhiteSpace(schema))
                throw new ArgumentException("Owned collection table schema cannot be whitespace.", nameof(schema));

            var prop = GetProperty(navigation);
            var ownedBuilder = new EntityTypeBuilder<TOwned>();
            buildAction?.Invoke(ownedBuilder);

            if (tableName == null && typeof(TOwned).Name.Contains('`'))
                throw new ArgumentException(
                    $"The owned type '{typeof(TOwned).Name}' is generic. " +
                    "Provide an explicit tableName when calling OwnsMany on a generic type.",
                    nameof(tableName));
            if (foreignKey == null && typeof(TEntity).Name.Contains('`'))
                throw new ArgumentException(
                    $"The owning type '{typeof(TEntity).Name}' is generic. " +
                    "Provide an explicit foreignKey when calling OwnsMany on a generic type.",
                    nameof(foreignKey));

            var childTable = tableName ?? typeof(TOwned).Name;
            var fkCol = foreignKey ?? typeof(TEntity).Name + "Id";
            // The owned type usually has no standalone Entity<TOwned>() registration, so any
            // infrastructure that resolves a mapping for it directly (the temporal bootstrap,
            // history-window reads) would fall back to the CLR-name table and target a table
            // that does not exist. Record the child table on the owned configuration so the
            // owned type's own mapping agrees with the owner's navigation.
            if (ownedBuilder.Configuration.TableName == null)
                ownedBuilder.ToTable(childTable, schema);
            _config.AddOwnedCollection(prop, new OwnedCollectionNavigation(typeof(TOwned), childTable, fkCol, ownedBuilder.Configuration)
            {
                SchemaName = schema
            });
            return this;
        }

        /// <summary>
        /// Begins configuration of a collection navigation property.
        /// </summary>
        /// <typeparam name="TProperty">The type of the dependent entity.</typeparam>
        /// <param name="navigation">Expression selecting the collection navigation.</param>
        /// <returns>A <see cref="CollectionNavigationBuilder{TProperty}"/> for relationship configuration.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="navigation"/> is null.</exception>
        public CollectionNavigationBuilder<TProperty> HasMany<TProperty>(Expression<Func<TEntity, IEnumerable<TProperty>>> navigation) where TProperty : class
        {
            ArgumentNullException.ThrowIfNull(navigation);
            var prop = GetProperty(navigation);
            return new CollectionNavigationBuilder<TProperty>(this, prop);
        }

        /// <summary>
        /// Begins configuration of a reference navigation property to a dependent entity.
        /// </summary>
        /// <typeparam name="TDependent">The dependent entity type.</typeparam>
        /// <param name="navigation">Expression selecting the reference navigation on the principal entity.</param>
        /// <returns>A <see cref="ReferenceNavigationBuilder{TDependent}"/> for relationship configuration.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="navigation"/> is null.</exception>
        public ReferenceNavigationBuilder<TDependent> HasOne<TDependent>(Expression<Func<TEntity, TDependent?>> navigation) where TDependent : class
        {
            ArgumentNullException.ThrowIfNull(navigation);
            var prop = GetProperty(navigation);
            return new ReferenceNavigationBuilder<TDependent>(this, prop);
        }

        private PropertyInfo GetProperty(Expression expression)
        {
            if (expression is MemberExpression me && me.Member is PropertyInfo meProp)
            {
                if (me.Expression is not ParameterExpression)
                    throw new ArgumentException(
                        $"Expression must be a direct property access (e => e.Property). " +
                        $"Chained or nested access such as 'e => e.Navigation.Property' is not supported. " +
                        $"Received expression tree: {expression}",
                        nameof(expression));
                return meProp;
            }

            if (expression is UnaryExpression ue &&
                (ue.NodeType == System.Linq.Expressions.ExpressionType.Convert ||
                 ue.NodeType == System.Linq.Expressions.ExpressionType.ConvertChecked) &&
                ue.Operand is MemberExpression ume && ume.Member is PropertyInfo umeProp)
            {
                if (ume.Expression is not ParameterExpression)
                    throw new ArgumentException(
                        $"Expression must be a direct property access (e => e.Property). " +
                        $"Chained access is not supported. Received expression tree: {expression}",
                        nameof(expression));
                return umeProp;
            }

            throw new ArgumentException(
                "Expression must select a single property (not a field, method call, or complex expression). " +
                "Received: " + expression.NodeType,
                nameof(expression));
        }

        private PropertyInfo GetProperty(LambdaExpression expression)
        {
            if (expression is null) throw new ArgumentNullException(nameof(expression));
            return GetProperty(expression.Body);
        }

        private IReadOnlyList<PropertyInfo> GetProperties(LambdaExpression expression)
        {
            if (expression is null) throw new ArgumentNullException(nameof(expression));

            static Expression UnwrapConvert(Expression e)
                => e is UnaryExpression ue &&
                   (ue.NodeType == System.Linq.Expressions.ExpressionType.Convert ||
                    ue.NodeType == System.Linq.Expressions.ExpressionType.ConvertChecked)
                    ? ue.Operand
                    : e;

            var body = UnwrapConvert(expression.Body);
            if (body is NewExpression ne)
            {
                if (ne.Arguments.Count == 0)
                    throw new ArgumentException("Composite key expression must select at least one property.", nameof(expression));

                var props = new PropertyInfo[ne.Arguments.Count];
                for (var i = 0; i < ne.Arguments.Count; i++)
                    props[i] = GetProperty(UnwrapConvert(ne.Arguments[i]));
                return props;
            }

            return new[] { GetProperty(body) };
        }

        private static string? NormalizeConstraintName(string? constraintName)
            => string.IsNullOrWhiteSpace(constraintName) ? null : constraintName;

    }
}
