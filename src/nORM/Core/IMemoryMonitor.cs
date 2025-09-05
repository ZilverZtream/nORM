namespace nORM.Core
{
    /// <summary>
    /// Provides information about current system memory usage.
    /// </summary>
    public interface IMemoryMonitor
    {
        /// <summary>
        /// Gets the amount of available memory in bytes.
        /// </summary>
        long GetAvailableMemory();
    }
}
