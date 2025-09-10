using System;
using System.Collections.Generic;

#nullable enable

namespace nORM.Configuration
{
    public class ModelBuilder
    {
        private readonly Dictionary<Type, IEntityTypeConfiguration> _configurations = new();

        public EntityTypeBuilder<TEntity> Entity<TEntity>() where TEntity : class
        {
            var builder = new EntityTypeBuilder<TEntity>();
            _configurations[typeof(TEntity)] = builder.Configuration;
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
        internal IEntityTypeConfiguration? GetConfiguration(Type type)
            => _configurations.TryGetValue(type, out var config) ? config : null;

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