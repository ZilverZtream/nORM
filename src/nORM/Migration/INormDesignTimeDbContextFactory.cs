using nORM.Core;

namespace nORM.Migration
{
    /// <summary>
    /// Creates a design-time <see cref="DbContext"/> for migration snapshot generation.
    /// </summary>
    /// <typeparam name="TContext">The concrete nORM context type.</typeparam>
    public interface INormDesignTimeDbContextFactory<out TContext>
        where TContext : DbContext
    {
        /// <summary>
        /// Creates a context instance for design-time tooling.
        /// </summary>
        /// <param name="args">Command-line arguments forwarded by tooling. nORM currently passes an empty array.</param>
        /// <returns>A configured context whose model should be used for migration generation.</returns>
        TContext CreateDbContext(string[] args);
    }
}
