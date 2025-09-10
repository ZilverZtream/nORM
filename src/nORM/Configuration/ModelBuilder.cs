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

        internal IEnumerable<Type> GetConfiguredEntityTypes()
            => _configurations.Keys;
    }
}