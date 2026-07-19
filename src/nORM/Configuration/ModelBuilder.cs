using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;

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
        /// Applies a dedicated <see cref="IEntityTypeConfiguration{TEntity}"/> to this model — the Entity
        /// Framework Core pattern for keeping per-entity configuration in its own class. The configuration's
        /// <see cref="IEntityTypeConfiguration{TEntity}.Configure"/> runs against the same builder that
        /// <see cref="Entity{TEntity}()"/> returns, so it accumulates with any other configuration for the type.
        /// </summary>
        /// <typeparam name="TEntity">The entity type being configured.</typeparam>
        /// <param name="configuration">The configuration to apply.</param>
        /// <returns>This <see cref="ModelBuilder"/> for chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="configuration"/> is null.</exception>
        public ModelBuilder ApplyConfiguration<TEntity>(IEntityTypeConfiguration<TEntity> configuration) where TEntity : class
        {
            ArgumentNullException.ThrowIfNull(configuration);
            configuration.Configure(Entity<TEntity>());
            return this;
        }

        /// <summary>
        /// Scans the given assembly for all non-abstract types implementing
        /// <see cref="IEntityTypeConfiguration{TEntity}"/> and applies each — the Entity Framework Core
        /// convenience that registers every configuration class in a project at once. Each configuration
        /// type must have a public parameterless constructor; a type implementing the interface for more than
        /// one entity is applied once per entity.
        /// </summary>
        /// <param name="assembly">The assembly to scan.</param>
        /// <returns>This <see cref="ModelBuilder"/> for chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="assembly"/> is null.</exception>
        [RequiresUnreferencedCode("Scans assembly types and their interfaces by reflection; trimming may remove configuration types or their members. See docs/aot-trimming.md.")]
        [RequiresDynamicCode("Constructs the generic ApplyConfiguration method per discovered entity type at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        public ModelBuilder ApplyConfigurationsFromAssembly(Assembly assembly)
        {
            ArgumentNullException.ThrowIfNull(assembly);

            Type[] types;
            try
            {
                types = assembly.GetTypes();
            }
            catch (ReflectionTypeLoadException ex)
            {
                // Use the loadable subset rather than failing the whole model when a sibling type
                // in the assembly cannot be loaded (missing dependency, etc.).
                types = (ex.Types ?? Array.Empty<Type?>()).Where(t => t != null).Select(t => t!).ToArray();
            }

            var applyMethod = typeof(ModelBuilder).GetMethod(nameof(ApplyConfiguration))!;
            foreach (var type in types)
            {
                if (type.IsAbstract || type.IsInterface || type.ContainsGenericParameters)
                    continue;
                foreach (var iface in type.GetInterfaces())
                {
                    if (!iface.IsGenericType || iface.GetGenericTypeDefinition() != typeof(IEntityTypeConfiguration<>))
                        continue;
                    var entityType = iface.GetGenericArguments()[0];
                    var configuration = Activator.CreateInstance(type);
                    applyMethod.MakeGenericMethod(entityType).Invoke(this, new[] { configuration });
                }
            }
            return this;
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