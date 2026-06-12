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
    public partial class EntityTypeBuilder<TEntity> where TEntity : class
    {
        /// <summary>
        /// Supports configuration of collection navigations to dependent entity types.
        /// </summary>
        /// <typeparam name="TDependent">The CLR type of the dependent entity.</typeparam>
        public class CollectionNavigationBuilder<TDependent> where TDependent : class
        {
            private readonly EntityTypeBuilder<TEntity> _parent;
            private readonly PropertyInfo _principalNavigation;

            internal CollectionNavigationBuilder(EntityTypeBuilder<TEntity> parent, PropertyInfo principalNavigation)
            {
                _parent = parent;
                _principalNavigation = principalNavigation;
            }

            /// <summary>
            /// Configures the relationship to include an optional reference navigation
            /// from the dependent entity back to the principal.
            /// </summary>
            /// <param name="navigation">Expression selecting the dependent's navigation property, if any.</param>
            /// <returns>A builder for configuring the relationship further.</returns>
            public ReferenceCollectionBuilder WithOne(Expression<Func<TDependent, TEntity?>>? navigation = null)
            {
                PropertyInfo? dependentNav = null;
                if (navigation != null)
                    dependentNav = _parent.GetProperty(navigation);
                return new ReferenceCollectionBuilder(_parent, _principalNavigation, dependentNav);
            }

            /// <summary>
            /// Begins configuration of a many-to-many relationship with an optional inverse navigation.
            /// </summary>
            /// <param name="inverseNavigation">Expression selecting the inverse collection property on <typeparamref name="TDependent"/>, if any.</param>
            /// <returns>A builder for specifying the join table details.</returns>
            public ManyToManyBuilder WithMany(Expression<Func<TDependent, IEnumerable<TEntity>>>? inverseNavigation = null)
            {
                string? inverseNavName = null;
                if (inverseNavigation != null)
                    inverseNavName = _parent.GetProperty(inverseNavigation).Name;
                return new ManyToManyBuilder(_parent, _principalNavigation, inverseNavName);
            }

            /// <summary>
            /// Supports fluent specification of join table details for a many-to-many relationship.
            /// </summary>
            public class ManyToManyBuilder
            {
                private readonly EntityTypeBuilder<TEntity> _parent;
                private readonly PropertyInfo _principalNavigation;
                private readonly string? _inverseNavName;

                internal ManyToManyBuilder(EntityTypeBuilder<TEntity> parent, PropertyInfo principalNavigation, string? inverseNavName)
                {
                    _parent = parent;
                    _principalNavigation = principalNavigation;
                    _inverseNavName = inverseNavName;
                }

                /// <summary>
                /// Specifies the join table, left FK column (this entity's PK) and right FK column (related entity's PK).
                /// </summary>
                /// <param name="joinTable">Name of the join table.</param>
                /// <param name="leftFk">Column referencing this entity's PK.</param>
                /// <param name="rightFk">Column referencing the related entity's PK.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
                /// <exception cref="ArgumentException">
                /// Thrown when any parameter is null or whitespace, or when <paramref name="leftFk"/>
                /// and <paramref name="rightFk"/> are the same column name.
                /// </exception>
                public EntityTypeBuilder<TEntity> UsingTable(string joinTable, string leftFk, string rightFk)
                    => UsingTable(joinTable, leftFk, rightFk, schema: null);

                /// <summary>
                /// Specifies a join table with ordered composite FK columns for this entity and the related entity.
                /// </summary>
                /// <param name="joinTable">Name of the join table.</param>
                /// <param name="leftFkColumns">Ordered columns referencing this entity's key.</param>
                /// <param name="rightFkColumns">Ordered columns referencing the related entity's key.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
                public EntityTypeBuilder<TEntity> UsingTable(string joinTable, IReadOnlyList<string> leftFkColumns, IReadOnlyList<string> rightFkColumns)
                    => UsingTable(joinTable, leftFkColumns, rightFkColumns, schema: null);

                /// <summary>
                /// Specifies the schema-qualified join table, left FK column (this entity's PK) and right FK column (related entity's PK).
                /// </summary>
                /// <param name="joinTable">Name of the join table without schema qualification.</param>
                /// <param name="leftFk">Column referencing this entity's PK.</param>
                /// <param name="rightFk">Column referencing the related entity's PK.</param>
                /// <param name="schema">Optional schema containing the join table.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
                /// <exception cref="ArgumentException">
                /// Thrown when any required parameter is null or whitespace, when <paramref name="schema"/>
                /// is whitespace, or when <paramref name="leftFk"/> and <paramref name="rightFk"/> are the same column name.
                /// </exception>
                public EntityTypeBuilder<TEntity> UsingTable(string joinTable, string leftFk, string rightFk, string? schema)
                {
                    if (string.IsNullOrWhiteSpace(joinTable))
                        throw new ArgumentException("Join table name cannot be null or whitespace.", nameof(joinTable));
                    if (string.IsNullOrWhiteSpace(leftFk))
                        throw new ArgumentException("Left FK column name cannot be null or whitespace.", nameof(leftFk));
                    if (string.IsNullOrWhiteSpace(rightFk))
                        throw new ArgumentException("Right FK column name cannot be null or whitespace.", nameof(rightFk));
                    if (schema is not null && string.IsNullOrWhiteSpace(schema))
                        throw new ArgumentException("Join table schema cannot be whitespace.", nameof(schema));
                    if (string.Equals(leftFk, rightFk, StringComparison.OrdinalIgnoreCase))
                        throw new ArgumentException(
                            $"The left FK column and right FK column in a many-to-many join table must be different. " +
                            $"Both were specified as '{leftFk}'. Provide distinct column names.",
                            nameof(rightFk));
                    _parent._config.AddManyToMany(new ManyToManyConfiguration(
                        _principalNavigation.Name,
                        typeof(TDependent),
                        joinTable,
                        leftFk,
                        rightFk,
                        _inverseNavName)
                    {
                        JoinTableSchema = schema,
                        LeftFkColumns = new[] { leftFk },
                        RightFkColumns = new[] { rightFk }
                    });
                    return _parent;
                }

                /// <summary>
                /// Specifies a join table whose columns reference alternate keys instead of primary keys.
                /// </summary>
                /// <param name="joinTable">Name of the join table without schema qualification.</param>
                /// <param name="leftFk">Column referencing this entity's selected key.</param>
                /// <param name="rightFk">Column referencing the related entity's selected key.</param>
                /// <param name="leftKey">Property on this entity referenced by <paramref name="leftFk"/>.</param>
                /// <param name="rightKey">Property on the related entity referenced by <paramref name="rightFk"/>.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
                public EntityTypeBuilder<TEntity> UsingTable(
                    string joinTable,
                    string leftFk,
                    string rightFk,
                    Expression<Func<TEntity, object>> leftKey,
                    Expression<Func<TDependent, object>> rightKey)
                    => UsingTable(joinTable, leftFk, rightFk, leftKey, rightKey, schema: null);

                /// <summary>
                /// Specifies a schema-qualified join table whose columns reference alternate keys instead of primary keys.
                /// </summary>
                /// <param name="joinTable">Name of the join table without schema qualification.</param>
                /// <param name="leftFk">Column referencing this entity's selected key.</param>
                /// <param name="rightFk">Column referencing the related entity's selected key.</param>
                /// <param name="leftKey">Property on this entity referenced by <paramref name="leftFk"/>.</param>
                /// <param name="rightKey">Property on the related entity referenced by <paramref name="rightFk"/>.</param>
                /// <param name="schema">Optional schema containing the join table.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
                public EntityTypeBuilder<TEntity> UsingTable(
                    string joinTable,
                    string leftFk,
                    string rightFk,
                    Expression<Func<TEntity, object>> leftKey,
                    Expression<Func<TDependent, object>> rightKey,
                    string? schema)
                {
                    ArgumentNullException.ThrowIfNull(leftKey);
                    ArgumentNullException.ThrowIfNull(rightKey);
                    return UsingTable(
                        joinTable,
                        new[] { leftFk },
                        new[] { rightFk },
                        leftKey,
                        rightKey,
                        schema);
                }

                /// <summary>
                /// Specifies a schema-qualified join table with ordered composite FK columns for both sides.
                /// </summary>
                /// <param name="joinTable">Name of the join table without schema qualification.</param>
                /// <param name="leftFkColumns">Ordered columns referencing this entity's key.</param>
                /// <param name="rightFkColumns">Ordered columns referencing the related entity's key.</param>
                /// <param name="schema">Optional schema containing the join table.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
                public EntityTypeBuilder<TEntity> UsingTable(string joinTable, IReadOnlyList<string> leftFkColumns, IReadOnlyList<string> rightFkColumns, string? schema)
                    => UsingTable(
                        joinTable,
                        leftFkColumns,
                        rightFkColumns,
                        ReferentialAction.NoAction,
                        ReferentialAction.NoAction,
                        ReferentialAction.NoAction,
                        ReferentialAction.NoAction,
                        schema);

                /// <summary>
                /// Specifies a schema-qualified join table with ordered composite FK columns and explicit
                /// database referential actions for both join-table foreign keys.
                /// </summary>
                /// <param name="joinTable">Name of the join table without schema qualification.</param>
                /// <param name="leftFkColumns">Ordered columns referencing this entity's key.</param>
                /// <param name="rightFkColumns">Ordered columns referencing the related entity's key.</param>
                /// <param name="leftOnDelete">Database action for deletes of this entity.</param>
                /// <param name="leftOnUpdate">Database action for key updates of this entity.</param>
                /// <param name="rightOnDelete">Database action for deletes of the related entity.</param>
                /// <param name="rightOnUpdate">Database action for key updates of the related entity.</param>
                /// <param name="schema">Optional schema containing the join table.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
                public EntityTypeBuilder<TEntity> UsingTable(
                    string joinTable,
                    IReadOnlyList<string> leftFkColumns,
                    IReadOnlyList<string> rightFkColumns,
                    ReferentialAction leftOnDelete,
                    ReferentialAction leftOnUpdate,
                    ReferentialAction rightOnDelete,
                    ReferentialAction rightOnUpdate,
                    string? schema = null)
                {
                    if (string.IsNullOrWhiteSpace(joinTable))
                        throw new ArgumentException("Join table name cannot be null or whitespace.", nameof(joinTable));
                    if (schema is not null && string.IsNullOrWhiteSpace(schema))
                        throw new ArgumentException("Join table schema cannot be whitespace.", nameof(schema));
                    ValidateCompositeFkColumns(leftFkColumns, nameof(leftFkColumns));
                    ValidateCompositeFkColumns(rightFkColumns, nameof(rightFkColumns));
                    _parent._config.AddManyToMany(new ManyToManyConfiguration(
                        _principalNavigation.Name,
                        typeof(TDependent),
                        joinTable,
                        leftFkColumns[0],
                        rightFkColumns[0],
                        _inverseNavName)
                    {
                        JoinTableSchema = schema,
                        LeftFkColumns = leftFkColumns.ToArray(),
                        RightFkColumns = rightFkColumns.ToArray(),
                        LeftOnDelete = leftOnDelete,
                        LeftOnUpdate = leftOnUpdate,
                        RightOnDelete = rightOnDelete,
                        RightOnUpdate = rightOnUpdate
                    });
                    return _parent;
                }

                /// <summary>
                /// Specifies a join table with ordered FK columns that reference selected single or composite keys.
                /// </summary>
                /// <param name="joinTable">Name of the join table without schema qualification.</param>
                /// <param name="leftFkColumns">Ordered columns referencing this entity's selected key.</param>
                /// <param name="rightFkColumns">Ordered columns referencing the related entity's selected key.</param>
                /// <param name="leftKey">Property or anonymous-object property list on this entity referenced by <paramref name="leftFkColumns"/>.</param>
                /// <param name="rightKey">Property or anonymous-object property list on the related entity referenced by <paramref name="rightFkColumns"/>.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
                public EntityTypeBuilder<TEntity> UsingTable(
                    string joinTable,
                    IReadOnlyList<string> leftFkColumns,
                    IReadOnlyList<string> rightFkColumns,
                    Expression<Func<TEntity, object>> leftKey,
                    Expression<Func<TDependent, object>> rightKey)
                    => UsingTable(joinTable, leftFkColumns, rightFkColumns, leftKey, rightKey, schema: null);

                /// <summary>
                /// Specifies a schema-qualified join table with ordered FK columns that reference selected single or composite keys.
                /// </summary>
                /// <param name="joinTable">Name of the join table without schema qualification.</param>
                /// <param name="leftFkColumns">Ordered columns referencing this entity's selected key.</param>
                /// <param name="rightFkColumns">Ordered columns referencing the related entity's selected key.</param>
                /// <param name="leftKey">Property or anonymous-object property list on this entity referenced by <paramref name="leftFkColumns"/>.</param>
                /// <param name="rightKey">Property or anonymous-object property list on the related entity referenced by <paramref name="rightFkColumns"/>.</param>
                /// <param name="schema">Optional schema containing the join table.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
                public EntityTypeBuilder<TEntity> UsingTable(
                    string joinTable,
                    IReadOnlyList<string> leftFkColumns,
                    IReadOnlyList<string> rightFkColumns,
                    Expression<Func<TEntity, object>> leftKey,
                    Expression<Func<TDependent, object>> rightKey,
                    string? schema)
                    => UsingTable(
                        joinTable,
                        leftFkColumns,
                        rightFkColumns,
                        leftKey,
                        rightKey,
                        ReferentialAction.NoAction,
                        ReferentialAction.NoAction,
                        ReferentialAction.NoAction,
                        ReferentialAction.NoAction,
                        schema);

                /// <summary>
                /// Specifies a schema-qualified join table with ordered FK columns that reference selected
                /// keys and explicit database referential actions for both join-table foreign keys.
                /// </summary>
                /// <param name="joinTable">Name of the join table without schema qualification.</param>
                /// <param name="leftFkColumns">Ordered columns referencing this entity's selected key.</param>
                /// <param name="rightFkColumns">Ordered columns referencing the related entity's selected key.</param>
                /// <param name="leftKey">Property or anonymous-object property list on this entity referenced by <paramref name="leftFkColumns"/>.</param>
                /// <param name="rightKey">Property or anonymous-object property list on the related entity referenced by <paramref name="rightFkColumns"/>.</param>
                /// <param name="leftOnDelete">Database action for deletes of this entity.</param>
                /// <param name="leftOnUpdate">Database action for key updates of this entity.</param>
                /// <param name="rightOnDelete">Database action for deletes of the related entity.</param>
                /// <param name="rightOnUpdate">Database action for key updates of the related entity.</param>
                /// <param name="schema">Optional schema containing the join table.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
                public EntityTypeBuilder<TEntity> UsingTable(
                    string joinTable,
                    IReadOnlyList<string> leftFkColumns,
                    IReadOnlyList<string> rightFkColumns,
                    Expression<Func<TEntity, object>> leftKey,
                    Expression<Func<TDependent, object>> rightKey,
                    ReferentialAction leftOnDelete,
                    ReferentialAction leftOnUpdate,
                    ReferentialAction rightOnDelete,
                    ReferentialAction rightOnUpdate,
                    string? schema = null)
                {
                    ArgumentNullException.ThrowIfNull(leftKey);
                    ArgumentNullException.ThrowIfNull(rightKey);
                    if (string.IsNullOrWhiteSpace(joinTable))
                        throw new ArgumentException("Join table name cannot be null or whitespace.", nameof(joinTable));
                    if (schema is not null && string.IsNullOrWhiteSpace(schema))
                        throw new ArgumentException("Join table schema cannot be whitespace.", nameof(schema));
                    ValidateCompositeFkColumns(leftFkColumns, nameof(leftFkColumns));
                    ValidateCompositeFkColumns(rightFkColumns, nameof(rightFkColumns));
                    var leftKeyProperties = _parent.GetProperties(leftKey);
                    var rightKeyProperties = _parent.GetProperties(rightKey);
                    if (leftKeyProperties.Count != leftFkColumns.Count)
                        throw new ArgumentException("Left FK column count must match the selected left key property count.", nameof(leftFkColumns));
                    if (rightKeyProperties.Count != rightFkColumns.Count)
                        throw new ArgumentException("Right FK column count must match the selected right key property count.", nameof(rightFkColumns));

                    _parent._config.AddManyToMany(new ManyToManyConfiguration(
                        _principalNavigation.Name,
                        typeof(TDependent),
                        joinTable,
                        leftFkColumns[0],
                        rightFkColumns[0],
                        _inverseNavName)
                    {
                        JoinTableSchema = schema,
                        LeftFkColumns = leftFkColumns.ToArray(),
                        RightFkColumns = rightFkColumns.ToArray(),
                        LeftKeyProperties = leftKeyProperties.ToArray(),
                        RightKeyProperties = rightKeyProperties.ToArray(),
                        LeftOnDelete = leftOnDelete,
                        LeftOnUpdate = leftOnUpdate,
                        RightOnDelete = rightOnDelete,
                        RightOnUpdate = rightOnUpdate
                    });
                    return _parent;
                }

                private static void ValidateCompositeFkColumns(IReadOnlyList<string>? columns, string paramName)
                {
                    if (columns is null || columns.Count == 0)
                        throw new ArgumentException("At least one FK column must be supplied.", paramName);
                    if (columns.Any(string.IsNullOrWhiteSpace))
                        throw new ArgumentException("FK column names cannot be null or whitespace.", paramName);
                    if (columns.Distinct(StringComparer.OrdinalIgnoreCase).Count() != columns.Count)
                        throw new ArgumentException("FK column names must be unique within each side of a many-to-many join table.", paramName);
                }
            }

            /// <summary>
            /// Enables configuration of relationship details between the principal
            /// and dependent types for a collection navigation.
            /// </summary>
            public class ReferenceCollectionBuilder
            {
                private readonly EntityTypeBuilder<TEntity> _parent;
                private readonly PropertyInfo _principalNavigation;
                private readonly PropertyInfo? _dependentNavigation;

                internal ReferenceCollectionBuilder(EntityTypeBuilder<TEntity> parent, PropertyInfo principalNavigation, PropertyInfo? dependentNavigation)
                {
                    _parent = parent;
                    _principalNavigation = principalNavigation;
                    _dependentNavigation = dependentNavigation;
                }

                /// <summary>
                /// Defines the foreign key used by the dependent entity in this relationship.
                /// </summary>
                /// <param name="foreignKeyExpression">Expression selecting the foreign key property on the dependent.</param>
                /// <param name="principalKeyExpression">Optional expression selecting the referenced principal key property.</param>
                /// <param name="cascadeDelete">Whether nORM should cascade deletes through the tracked object graph.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for further configuration.</returns>
                /// <exception cref="ArgumentNullException">Thrown when <paramref name="foreignKeyExpression"/> is null.</exception>
                /// <exception cref="NormConfigurationException">Thrown when the principal key cannot be inferred.</exception>
                public EntityTypeBuilder<TEntity> HasForeignKey(
                    Expression<Func<TDependent, object>> foreignKeyExpression,
                    Expression<Func<TEntity, object>>? principalKeyExpression = null,
                    bool cascadeDelete = true)
                {
                    var (principalKeys, fkProps) = ResolveForeignKeyProperties(foreignKeyExpression, principalKeyExpression);
                    _parent._config.AddRelationship(new RelationshipConfiguration(
                        _principalNavigation,
                        typeof(TDependent),
                        _dependentNavigation,
                        principalKeys,
                        fkProps,
                        cascadeDelete));
                    return _parent;
                }

                /// <summary>
                /// Defines the foreign key used by the dependent entity and preserves an explicit database constraint name.
                /// </summary>
                /// <param name="foreignKeyExpression">Expression selecting the foreign key property on the dependent.</param>
                /// <param name="principalKeyExpression">Optional expression selecting the referenced principal key property.</param>
                /// <param name="constraintName">Database foreign key constraint name to preserve in migration snapshots.</param>
                /// <param name="cascadeDelete">Whether nORM should cascade deletes through the tracked object graph.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for further configuration.</returns>
                /// <exception cref="ArgumentNullException">Thrown when <paramref name="foreignKeyExpression"/> is null.</exception>
                /// <exception cref="NormConfigurationException">Thrown when the principal key cannot be inferred.</exception>
                public EntityTypeBuilder<TEntity> HasForeignKey(
                    Expression<Func<TDependent, object>> foreignKeyExpression,
                    Expression<Func<TEntity, object>>? principalKeyExpression,
                    string? constraintName,
                    bool cascadeDelete = true)
                {
                    var (principalKeys, fkProps) = ResolveForeignKeyProperties(foreignKeyExpression, principalKeyExpression);
                    _parent._config.AddRelationship(new RelationshipConfiguration(
                        _principalNavigation,
                        typeof(TDependent),
                        _dependentNavigation,
                        principalKeys,
                        fkProps,
                        cascadeDelete)
                    {
                        ConstraintName = NormalizeConstraintName(constraintName)
                    });
                    return _parent;
                }

                /// <summary>
                /// Defines the foreign key used by the dependent entity and the database
                /// referential actions emitted for migration snapshots and provider DDL.
                /// </summary>
                /// <param name="foreignKeyExpression">Expression selecting the foreign key property on the dependent.</param>
                /// <param name="principalKeyExpression">Optional expression selecting the referenced principal key property.</param>
                /// <param name="onDelete">Database action for principal deletes.</param>
                /// <param name="onUpdate">Database action for principal key updates.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for further configuration.</returns>
                /// <exception cref="ArgumentNullException">Thrown when <paramref name="foreignKeyExpression"/> is null.</exception>
                /// <exception cref="NormConfigurationException">Thrown when the principal key cannot be inferred.</exception>
                public EntityTypeBuilder<TEntity> HasForeignKey(
                    Expression<Func<TDependent, object>> foreignKeyExpression,
                    Expression<Func<TEntity, object>>? principalKeyExpression,
                    ReferentialAction onDelete,
                    ReferentialAction onUpdate)
                {
                    var (principalKeys, fkProps) = ResolveForeignKeyProperties(foreignKeyExpression, principalKeyExpression);
                    _parent._config.AddRelationship(new RelationshipConfiguration(
                        _principalNavigation,
                        typeof(TDependent),
                        _dependentNavigation,
                        principalKeys,
                        fkProps,
                        onDelete,
                        onUpdate));
                    return _parent;
                }

                /// <summary>
                /// Defines the foreign key used by the dependent entity, explicit database referential actions,
                /// and an explicit database constraint name.
                /// </summary>
                /// <param name="foreignKeyExpression">Expression selecting the foreign key property on the dependent.</param>
                /// <param name="principalKeyExpression">Optional expression selecting the referenced principal key property.</param>
                /// <param name="onDelete">Database action for principal deletes.</param>
                /// <param name="onUpdate">Database action for principal key updates.</param>
                /// <param name="constraintName">Database foreign key constraint name to preserve in migration snapshots.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for further configuration.</returns>
                /// <exception cref="ArgumentNullException">Thrown when <paramref name="foreignKeyExpression"/> is null.</exception>
                /// <exception cref="NormConfigurationException">Thrown when the principal key cannot be inferred.</exception>
                public EntityTypeBuilder<TEntity> HasForeignKey(
                    Expression<Func<TDependent, object>> foreignKeyExpression,
                    Expression<Func<TEntity, object>>? principalKeyExpression,
                    ReferentialAction onDelete,
                    ReferentialAction onUpdate,
                    string? constraintName)
                {
                    var (principalKeys, fkProps) = ResolveForeignKeyProperties(foreignKeyExpression, principalKeyExpression);
                    _parent._config.AddRelationship(new RelationshipConfiguration(
                        _principalNavigation,
                        typeof(TDependent),
                        _dependentNavigation,
                        principalKeys,
                        fkProps,
                        onDelete,
                        onUpdate)
                    {
                        ConstraintName = NormalizeConstraintName(constraintName)
                    });
                    return _parent;
                }

                private (IReadOnlyList<PropertyInfo> PrincipalKeys, IReadOnlyList<PropertyInfo> ForeignKeys) ResolveForeignKeyProperties(
                    Expression<Func<TDependent, object>> foreignKeyExpression,
                    Expression<Func<TEntity, object>>? principalKeyExpression)
                {
                    ArgumentNullException.ThrowIfNull(foreignKeyExpression);
                    var fkProps = _parent.GetProperties(foreignKeyExpression);
                    IReadOnlyList<PropertyInfo> principalKeys;
                    if (principalKeyExpression != null)
                    {
                        principalKeys = _parent.GetProperties(principalKeyExpression);
                    }
                    else if (_parent._config.KeyProperties.Count == fkProps.Count)
                    {
                        principalKeys = _parent._config.KeyProperties.ToArray();
                    }
                    else
                    {
                        throw new NormConfigurationException(string.Format(ErrorMessages.InvalidConfiguration, $"Principal key must be specified for relationship '{_principalNavigation.Name}' on entity {typeof(TEntity).Name}"));
                    }

                    if (principalKeys.Count != fkProps.Count)
                        throw new NormConfigurationException(string.Format(ErrorMessages.InvalidConfiguration,
                            $"Relationship '{_principalNavigation.Name}' on entity {typeof(TEntity).Name} has {principalKeys.Count} principal key properties but {fkProps.Count} foreign key properties."));

                    return (principalKeys, fkProps);
                }

            }
        }
    }
}
