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
        /// Provides configuration options for a specific property on the entity type.
        /// </summary>
        public class PropertyBuilder
        {
            /// <summary>The parent entity type builder that owns this property builder.</summary>
            protected readonly EntityTypeBuilder<TEntity> _parent;
            /// <summary>Reflection metadata for the property being configured.</summary>
            protected readonly PropertyInfo _property;

            internal PropertyBuilder(EntityTypeBuilder<TEntity> parent, PropertyInfo property)
            {
                _parent = parent;
                _property = property;
            }

            /// <summary>
            /// Sets the database column name for the configured property.
            /// </summary>
            /// <param name="name">The column name to map the property to.</param>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> is null or whitespace.</exception>
            public PropertyBuilder HasColumnName(string name)
            {
                // Validation delegated to SetColumnName
                _parent._config.SetColumnName(_property, name);
                return this;
            }

            /// <summary>
            /// Configures SQL default metadata for this property for migration snapshot
            /// generation. This does not mark the property as database-generated and does
            /// not cause nORM to omit the column from INSERT statements.
            /// </summary>
            /// <param name="sql">A SQL literal or no-argument function accepted by migration default validation.</param>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            /// <exception cref="ArgumentException">Thrown when <paramref name="sql"/> is null or whitespace.</exception>
            public PropertyBuilder HasDefaultValueSql(string sql)
            {
                _parent._config.SetDefaultValueSql(_property, sql);
                return this;
            }

            /// <summary>
            /// Configures SQL default metadata and preserves an optional provider default-constraint name.
            /// This does not mark the property as database-generated and does not cause nORM to omit the column from INSERT statements.
            /// </summary>
            /// <param name="sql">A SQL literal or no-argument function accepted by migration default validation.</param>
            /// <param name="constraintName">Optional provider default-constraint name to preserve in migration DDL.</param>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            /// <exception cref="ArgumentException">Thrown when <paramref name="sql"/> is null or whitespace.</exception>
            public PropertyBuilder HasDefaultValueSql(string sql, string? constraintName)
            {
                _parent._config.SetDefaultValueSql(_property, sql, constraintName);
                return this;
            }

            /// <summary>
            /// Configures provider identity seed/increment metadata for migration
            /// generation. Providers without matching identity DDL may ignore or reject
            /// the metadata during migration SQL generation.
            /// </summary>
            public PropertyBuilder HasIdentityOptions(long seed, long increment)
            {
                _parent._config.SetIdentityOptions(_property, seed, increment);
                return this;
            }

            /// <summary>
            /// Configures maximum length metadata for string or byte array columns in
            /// migration snapshots.
            /// </summary>
            /// <param name="length">Maximum length to use in provider DDL when supported.</param>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="length"/> is less than one.</exception>
            /// <exception cref="ArgumentException">Thrown when the property is not a string or byte array.</exception>
            public PropertyBuilder HasMaxLength(int length)
            {
                _parent._config.SetMaxLength(_property, length);
                return this;
            }

            /// <summary>
            /// Configures whether the column is required (non-nullable) in the generated schema
            /// (EnsureCreated / migrations) — the Entity Framework Core fluent equivalent of a NOT NULL
            /// constraint. This overrides the CLR/attribute-derived nullability used for schema generation;
            /// runtime query nullability remains driven by the CLR type.
            /// </summary>
            /// <param name="required">True to make the column NOT NULL; false to allow NULL.</param>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            public PropertyBuilder IsRequired(bool required = true)
            {
                _parent._config.SetRequired(_property, required);
                return this;
            }

            /// <summary>
            /// Configures the property as store-generated on INSERT (EF Core's <c>ValueGeneratedOnAdd</c>) —
            /// e.g. an identity/serial key or a column with a database default. The column is omitted from
            /// generated INSERT statements so the database supplies the value.
            /// </summary>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            public PropertyBuilder ValueGeneratedOnAdd()
            {
                _parent._config.SetValueGenerated(_property, System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption.Identity);
                return this;
            }

            /// <summary>
            /// Configures the property as never store-generated (EF Core's <c>ValueGeneratedNever</c>): nORM
            /// always writes the CLR value, clearing any attribute-derived database-generated flag.
            /// </summary>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            public PropertyBuilder ValueGeneratedNever()
            {
                _parent._config.SetValueGenerated(_property, System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption.None);
                return this;
            }

            /// <summary>
            /// Configures the property as store-generated on INSERT and UPDATE (EF Core's
            /// <c>ValueGeneratedOnAddOrUpdate</c>) — a computed column. It is omitted from generated INSERT and
            /// UPDATE statements so the database supplies the value.
            /// </summary>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            public PropertyBuilder ValueGeneratedOnAddOrUpdate()
            {
                _parent._config.SetValueGenerated(_property, System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption.Computed);
                return this;
            }

            /// <summary>
            /// Configures the property as the entity's row-version / optimistic-concurrency token (EF Core's
            /// <c>IsRowVersion</c>) — the fluent equivalent of <c>[Timestamp]</c>. nORM manages the token and
            /// adds it to the UPDATE concurrency check, so a stale write is rejected instead of silently
            /// overwriting a concurrent change.
            /// </summary>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            public PropertyBuilder IsRowVersion()
            {
                _parent._config.SetRowVersion(_property);
                return this;
            }

            /// <summary>
            /// Configures the explicit provider store type for the column (EF Core's <c>HasColumnType</c>,
            /// e.g. <c>"decimal(18,2)"</c>, <c>"nvarchar(max)"</c>, <c>"jsonb"</c>). The migration SQL
            /// generators (EnsureCreated / migrations) emit it verbatim instead of deriving the type from the
            /// CLR type and the length/precision facets.
            /// </summary>
            /// <param name="typeName">The provider store type to use.</param>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            /// <exception cref="ArgumentException">Thrown when <paramref name="typeName"/> is null or whitespace.</exception>
            public PropertyBuilder HasColumnType(string typeName)
            {
                _parent._config.SetColumnType(_property, typeName);
                return this;
            }

            /// <summary>
            /// Configures whether this string property uses Unicode-capable storage.
            /// </summary>
            /// <param name="unicode">True for Unicode text storage; false for non-Unicode text storage.</param>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            /// <exception cref="ArgumentException">Thrown when the property is not a string.</exception>
            public PropertyBuilder IsUnicode(bool unicode = true)
            {
                _parent._config.SetUnicode(_property, unicode);
                return this;
            }

            /// <summary>
            /// Configures whether this string or byte array property uses fixed-length storage.
            /// </summary>
            /// <param name="fixedLength">True for fixed-length storage; false for variable-length storage.</param>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            /// <exception cref="ArgumentException">Thrown when the property is not a string or byte array.</exception>
            public PropertyBuilder IsFixedLength(bool fixedLength = true)
            {
                _parent._config.SetFixedLength(_property, fixedLength);
                return this;
            }

            /// <summary>
            /// Configures decimal precision metadata for migration snapshots.
            /// </summary>
            /// <param name="precision">Total number of decimal digits.</param>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="precision"/> is less than one.</exception>
            /// <exception cref="ArgumentException">Thrown when the property is not decimal.</exception>
            public PropertyBuilder HasPrecision(int precision)
            {
                _parent._config.SetPrecision(_property, precision, scale: null);
                return this;
            }

            /// <summary>
            /// Configures decimal precision and scale metadata for migration snapshots.
            /// </summary>
            /// <param name="precision">Total number of decimal digits.</param>
            /// <param name="scale">Number of digits after the decimal point.</param>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            /// <exception cref="ArgumentOutOfRangeException">Thrown when precision or scale is invalid.</exception>
            /// <exception cref="ArgumentException">Thrown when the property is not decimal.</exception>
            public PropertyBuilder HasPrecision(int precision, int scale)
            {
                _parent._config.SetPrecision(_property, precision, scale);
                return this;
            }

            /// <summary>
            /// Configures the database collation for this property in migration snapshots.
            /// The value is a provider collation identifier such as <c>NOCASE</c>,
            /// <c>Latin1_General_100_CI_AS</c>, or <c>utf8mb4_0900_ai_ci</c>.
            /// </summary>
            /// <param name="collation">Provider collation identifier.</param>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            /// <exception cref="ArgumentException">Thrown when <paramref name="collation"/> is null or whitespace.</exception>
            public PropertyBuilder HasCollation(string collation)
            {
                _parent._config.SetCollation(_property, collation);
                return this;
            }

            /// <summary>
            /// Configures this property as a database-computed/generated column for
            /// migration snapshot generation. The SQL expression is provider SQL and
            /// should not include the outer generated-column syntax.
            /// </summary>
            /// <param name="sql">Provider SQL expression used to compute the column.</param>
            /// <param name="stored">Whether the generated value should be physically stored when the provider supports that choice.</param>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            /// <exception cref="ArgumentException">Thrown when <paramref name="sql"/> is null or whitespace.</exception>
            public PropertyBuilder HasComputedColumnSql(string sql, bool stored = false)
            {
                _parent._config.SetComputedColumnSql(_property, sql, stored);
                return this;
            }

            /// <summary>
            /// Configures a value converter for this property using the untyped interface.
            /// </summary>
            /// <param name="converter">The value converter to apply.</param>
            /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
            /// <exception cref="ArgumentNullException">Thrown when <paramref name="converter"/> is null.</exception>
            public EntityTypeBuilder<TEntity> HasConversion(IValueConverter converter)
            {
                ArgumentNullException.ThrowIfNull(converter);
                _parent._config.AddConverter(_property, converter);
                return _parent;
            }

            /// <summary>
            /// Returns the parent <see cref="EntityTypeBuilder{TEntity}"/> to resume entity-level chaining.
            /// </summary>
            public EntityTypeBuilder<TEntity> Builder => _parent;
        }

        /// <summary>
        /// Strongly-typed property builder that enables type-safe value converter configuration.
        /// </summary>
        /// <typeparam name="TProperty">The CLR type of the property being configured.</typeparam>
        public class PropertyBuilder<TProperty> : PropertyBuilder
        {
            internal PropertyBuilder(EntityTypeBuilder<TEntity> parent, PropertyInfo property)
                : base(parent, property) { }

            /// <summary>
            /// Configures a strongly-typed value converter for this property.
            /// </summary>
            /// <typeparam name="TProvider">The provider/database storage type.</typeparam>
            /// <param name="converter">The strongly-typed value converter.</param>
            /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
            public EntityTypeBuilder<TEntity> HasConversion<TProvider>(ValueConverter<TProperty, TProvider> converter)
                => HasConversion((IValueConverter)converter);
        }

        /// <summary>
        /// Configures a shadow property that exists only in the model and database.
        /// </summary>
        public class ShadowPropertyBuilder
        {
            private readonly EntityTypeBuilder<TEntity> _parent;
            private readonly string _name;

            internal ShadowPropertyBuilder(EntityTypeBuilder<TEntity> parent, string name)
            {
                _parent = parent;
                _name = name;
            }

            /// <summary>
            /// Assigns a column name for the shadow property.
            /// </summary>
            /// <param name="columnName">The column name to use in the database.</param>
            /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
            /// <exception cref="ArgumentException">Thrown when <paramref name="columnName"/> is null or whitespace.</exception>
            public EntityTypeBuilder<TEntity> HasColumnName(string columnName)
            {
                // Validation delegated to SetShadowColumnName
                _parent._config.SetShadowColumnName(_name, columnName);
                return _parent;
            }
        }
    }
}
