using System;
using System.Collections.Generic;

#nullable enable

namespace nORM.Configuration
{
    /// <summary>
    /// Collects entity type configuration metadata using a fluent API.
    /// </summary>
    public class ModelBuilder
    {
        private readonly Dictionary<Type, IEntityTypeConfiguration> _configurations = new();
        private readonly Dictionary<Type, object> _builders = new();

        /// <summary>
        /// Begins configuration for the specified entity CLR type.
        /// Repeated calls for the same type return the same builder so that
        /// multiple chained configurations are accumulated rather than replaced.
        /// </summary>
        /// <typeparam name="TEntity">The entity type to configure.</typeparam>
        /// <returns>An <see cref="EntityTypeBuilder{TEntity}"/> for configuring the entity.</returns>
        public EntityTypeBuilder<TEntity> Entity<TEntity>() where TEntity : class
        {
            if (_builders.TryGetValue(typeof(TEntity), out var existing))
                return (EntityTypeBuilder<TEntity>)existing;
            var builder = new EntityTypeBuilder<TEntity>();
            _configurations[typeof(TEntity)] = builder.Configuration;
            _builders[typeof(TEntity)] = builder;
            return builder;
        }

        /// <summary>
        /// Retrieves the configuration associated with a given entity type if one has been registered.
        /// </summary>
        /// <param name="type">The CLR type representing the entity.</param>
        /// <returns>
        /// The <see cref="IEntityTypeConfiguration"/> for the specified type, or <c>null</c> if the type
        /// has not been configured.
        /// </returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="type"/> is null.</exception>
        internal IEntityTypeConfiguration? GetConfiguration(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);
            if (_configurations.TryGetValue(type, out var config))
                return config;

            // A type that appears only as an owned collection has no standalone
            // Entity<T>() registration. Fall back to the owned navigation's configuration
            // (which carries the configured child table) so infrastructure resolving a
            // mapping for the owned type directly — the temporal bootstrap, history-window
            // reads — targets the configured owned table instead of the CLR-name default.
            // Ambiguity across owners with DIFFERENT tables keeps the legacy behavior.
            IEntityTypeConfiguration? found = null;
            string? foundTable = null;
            foreach (var ownerConfig in _configurations.Values)
            {
                foreach (var nav in ownerConfig.OwnedCollectionNavigations.Values)
                {
                    if (nav.OwnedType != type || nav.Configuration == null) continue;
                    if (foundTable != null && !string.Equals(foundTable, nav.TableName, StringComparison.Ordinal))
                        return null;
                    found = nav.Configuration;
                    foundTable = nav.TableName;
                }
            }
            return found;
        }

        /// <summary>
        /// Enumerates all entity CLR types that have been explicitly configured
        /// using <see cref="Entity{TEntity}()"/>. The resulting sequence can be
        /// used by infrastructure components to build mappings or perform
        /// additional model validation at runtime.
        /// </summary>
        /// <returns>An enumerable collection of configured entity types.</returns>
        internal IEnumerable<Type> GetConfiguredEntityTypes()
            => _configurations.Keys;
    }
}