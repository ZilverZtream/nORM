#nullable enable

namespace nORM.Configuration
{
    /// <summary>
    /// Configures a single entity type's mapping in a dedicated class — the Entity Framework Core pattern.
    /// Implement this interface, then apply it with
    /// <see cref="ModelBuilder.ApplyConfiguration{TEntity}(IEntityTypeConfiguration{TEntity})"/> or
    /// <see cref="ModelBuilder.ApplyConfigurationsFromAssembly(System.Reflection.Assembly)"/>. This keeps
    /// per-entity fluent configuration out of one large <c>OnModelCreating</c> override.
    /// </summary>
    /// <typeparam name="TEntity">The entity type this configuration applies to.</typeparam>
    public interface IEntityTypeConfiguration<TEntity> where TEntity : class
    {
        /// <summary>
        /// Configures the entity of type <typeparamref name="TEntity"/>.
        /// </summary>
        /// <param name="builder">The builder used to configure the entity type.</param>
        void Configure(EntityTypeBuilder<TEntity> builder);
    }
}
