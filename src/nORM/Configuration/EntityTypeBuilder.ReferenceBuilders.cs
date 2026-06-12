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
        /// Supports configuration of reference navigations to dependent entity types.
        /// </summary>
        /// <typeparam name="TDependent">The CLR type of the dependent entity.</typeparam>
        public class ReferenceNavigationBuilder<TDependent> where TDependent : class
        {
            private readonly EntityTypeBuilder<TEntity> _parent;
            private readonly PropertyInfo _principalNavigation;

            internal ReferenceNavigationBuilder(EntityTypeBuilder<TEntity> parent, PropertyInfo principalNavigation)
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
            public ReferenceReferenceBuilder WithOne(Expression<Func<TDependent, TEntity?>>? navigation = null)
            {
                PropertyInfo? dependentNav = null;
                if (navigation != null)
                    dependentNav = _parent.GetProperty(navigation);
                return new ReferenceReferenceBuilder(_parent, _principalNavigation, dependentNav);
            }

            /// <summary>
            /// Enables configuration of relationship details between the principal
            /// and dependent types for a reference navigation.
            /// </summary>
            public class ReferenceReferenceBuilder
            {
                private readonly EntityTypeBuilder<TEntity> _parent;
                private readonly PropertyInfo _principalNavigation;
                private readonly PropertyInfo? _dependentNavigation;

                internal ReferenceReferenceBuilder(EntityTypeBuilder<TEntity> parent, PropertyInfo principalNavigation, PropertyInfo? dependentNavigation)
                {
                    _parent = parent;
                    _principalNavigation = principalNavigation;
                    _dependentNavigation = dependentNavigation;
                }

                /// <summary>
                /// Defines the foreign key used by the dependent entity in this one-to-one relationship.
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
                    var principalKeys = principalKeyExpression is not null
                        ? _parent.GetProperties(principalKeyExpression)
                        : _parent._config.KeyProperties;

                    if (principalKeys.Count == 0)
                    {
                        throw new NormConfigurationException(
                            $"Relationship '{_principalNavigation.Name}' on entity {typeof(TEntity).Name} cannot infer a principal key. " +
                            "Configure HasKey(...) first or pass the principal key expression to HasForeignKey(...).");
                    }

                    if (principalKeys.Count != fkProps.Count)
                    {
                        throw new ArgumentException(
                            $"Relationship '{_principalNavigation.Name}' has {principalKeys.Count} principal key property/properties but {fkProps.Count} foreign key property/properties.",
                            nameof(foreignKeyExpression));
                    }

                    return (principalKeys, fkProps);
                }
            }
        }
    }
}
